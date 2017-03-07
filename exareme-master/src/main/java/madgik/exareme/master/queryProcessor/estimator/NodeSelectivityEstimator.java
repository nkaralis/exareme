package madgik.exareme.master.queryProcessor.estimator;

import com.google.gson.Gson;

import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.estimator.db.AttrInfo;
import madgik.exareme.master.queryProcessor.estimator.db.RelInfo;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.master.queryProcessor.estimator.histogram.Histogram;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author jim
 */
public class NodeSelectivityEstimator implements SelectivityEstimator {
	private static final int HASH_STRING_CHARS = 11;
	private static final int HASH_STRING_BASE = 256;

	private Schema schema;
	private static final org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger(NodeSelectivityEstimator.class);

	public NodeSelectivityEstimator(String json) throws Exception {
		BufferedReader br;
		br = new BufferedReader(new FileReader(json));

		// convert the json string back to object
		Gson gson = new Gson();
		schema = gson.fromJson(br, Schema.class);

		// System.out.println(schema);

		// HashMap<String, HashSet<String>> keys = new HashMap<String,
		// HashSet<String>>();
		// try {
		// keys =
		// di.madgik.decomposer.util.Util.getMysqlIndices("jdbc:mysql://10.240.0.10:3306/npd?"
		// + "user=benchmark&password=pass");
		// } catch (SQLException ex) {
		// Logger.getLogger(NodeSelectivityEstimator.class.getName()).log(Level.SEVERE,
		// null, ex);
		// }
		// for (String table : keys.keySet()) {
		// if(schema.getTableIndex().containsKey(table)){
		// schema.getTableIndex().get(table).setHashAttr(keys.get(table));
		// schema.getTableIndex().get(table).setNumberOfPartitions(1);}
		// else{
		// Logger.getLogger(NodeSelectivityEstimator.class.getName()).log(Level.WARNING,
		// "Table {0} does not exist in stat json file", table);
		// }
		// }

	}

	@Override
	public void makeEstimationForNode(Node n) {
		try {
			if (!n.getObject().toString().startsWith("table")) {
				estimateBase(n);
			} else {
				Node o = n.getChildAt(0);
				if (o.getOpCode() == Node.JOIN) {
					NonUnaryWhereCondition bwc = (NonUnaryWhereCondition) o.getObject();
					if (o.getChildren().size() == 1) {
						estimateFilterJoin(n, bwc);
					} else {
						estimateJoin(n, bwc, o.getChildAt(0), o.getChildAt(1));
					}
				} else if (o.getOpCode() == Node.PROJECT || o.getOpCode() == Node.BASEPROJECT) {
					estimateProject(n);
				} else if (o.getOpCode() == Node.SELECT) {
					Selection s = (Selection) o.getObject();
					estimateFilter(n, s, o.getChildAt(0));
				} else if (o.getOpCode() == Node.UNION) {
					estimateUnion(n);
				}
			}
		} catch (Exception ex) {
			System.out
					.println("cannot compute selectivity for node " + n.getObject().toString() + ":" + ex.getMessage());
			log.error("cannot compute selectivity for node " + n.getObject().toString() + ":" + ex.getMessage());
		}

	}

	/*public void estimateFilter(Node n, Selection s, Node child) {
		// Selection s = (Selection) n.getObject();
		NodeInfo ni = new NodeInfo();
		n.setNodeInfo(ni);
		NodeInfo childInfo = child.getNodeInfo();
		Set<Operand> filters = s.getOperands();

		// RelInfo initRel = childInfo.getResultRel();
		ni.setNumberOfTuples(childInfo.getNumberOfTuples());
		ni.setTupleLength(childInfo.getTupleLength());
		ni.setResultRel(new RelInfo(childInfo.getResultRel()));

		// one select node can contain more than one filter!
		for (Operand nextFilter : filters) {
			applyFilterToNode(nextFilter, n);
		}

	}*/
	public void estimateFilter(Node n, Selection s, Node child) {
		// Selection s = (Selection) n.getObject();
		NodeInfo ni = new NodeInfo();
		n.setNodeInfo(ni);
		NodeInfo childInfo = child.getNodeInfo();

		Set<Operand> filters = s.getOperands();

		// RelInfo initRel = childInfo.getResultRel();
		ni.setNumberOfTuples(childInfo.getNumberOfTuples());
		ni.setTupleLength(childInfo.getTupleLength());
		ni.setResultRel(new RelInfo(childInfo.getResultRel()));

		// one select node can contain more than one filter!
		for (Operand nextFilter : filters) {
			applyFilterToNode(nextFilter, ni, child);
		}

	}

	/*private void applyFilterToNode(Operand nextFilter, Node n) {
		NodeInfo ni = n.getNodeInfo();
		if (nextFilter instanceof BinaryOperand) {
			BinaryOperand bo = (BinaryOperand) nextFilter;
			NonUnaryWhereCondition nuwc = new NonUnaryWhereCondition();
			nuwc.setLeftOp(bo.getLeftOp());
			nuwc.setRightOp(bo.getRightOp());
			nuwc.setOperator(bo.getOperator());
			applyFilterToNode(nuwc, n);
			return;
		}
		if (nextFilter instanceof UnaryWhereCondition) {
			// normally you don't care for these conditions (Column IS NOT
			// NULL)
			// UnaryWhereCondition uwc = (UnaryWhereCondition) nextFilter;
			// Table t=(Table) child.getObject();
			// if(t.getName().startsWith("table")){
			// not base table

			// TODO: fix nodeInfo
			// do nothing!

			// this.planInfo.get(n.getHashId()).setNumberOfTuples(child.getNumberOfTuples());
			// this.planInfo.get(n.getHashId()).setTupleLength(child.getTupleLength());
			// System.out.println(uwc);
		} else if (nextFilter instanceof NonUnaryWhereCondition) {
			// TODO correct this!
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) nextFilter;
			// System.out.println(nuwc);
			String operator = nuwc.getOperator(); // e.g. =, <, >

			if (operator.equalsIgnoreCase("and")) {
				applyFilterToNode(nuwc.getLeftOp(), n);
				applyFilterToNode(nuwc.getRightOp(), n);
			} else if (operator.equalsIgnoreCase("or")) {
				log.warn("Filter with OR condition " + nuwc.toString()
						+ ". Selectivity estimation will not be accurate");
				// TODO fix estimation with OR conditions
				// for now just return the one with higher cardinality
				NodeInfo left = new NodeInfo();
				Node dummyLeft = new Node(Node.OR);
				dummyLeft.setNodeInfo(left);
				left.setNumberOfTuples(ni.getNumberOfTuples());
				left.setTupleLength(ni.getTupleLength());
				left.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), dummyLeft);

				NodeInfo right = new NodeInfo();
				Node dummyRight = new Node(Node.OR);
				dummyRight.setNodeInfo(right);
				right.setNumberOfTuples(ni.getNumberOfTuples());
				right.setTupleLength(ni.getTupleLength());
				right.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), dummyRight);

				if (left.getNumberOfTuples() > right.getNumberOfTuples()) {
					n.setNodeInfo(left);
				} else {
					n.setNodeInfo(right);
				}
			} else {

				Column col;
				Constant con;

				if (nuwc.getLeftOp() instanceof Column && nuwc.getRightOp() instanceof Column) {
					estimateFilterJoin(n, nuwc);
				} else {
					if (nuwc.getLeftOp() instanceof Column) {// TODO: constant
						col = (Column) nuwc.getLeftOp();
						con = (Constant) nuwc.getRightOp();
					} else {
						col = (Column) nuwc.getRightOp();
						con = (Constant) nuwc.getLeftOp();
					}

					// RelInfo lRel =
					// this.schema.getTableIndex().get(col.tableAlias);
					// RelInfo lRel = childInfo.getResultRel();
					// RelInfo resultRel = new RelInfo(lRel);
					// RelInfo resultRel = initRel;

					Histogram resultHistogram = ni.getResultRel().getAttrIndex().get(col.getName()).getHistogram();

					double filterValue = 0;
					if (!con.isArithmetic()) {
						if (con.getValue() instanceof String) {
							String st = (String) con.getValue();
							filterValue = StatUtils.hashString(con.getValue().toString());
							String newSt = "";
							if (st.startsWith("\'")) {
								newSt = st.replaceAll("\'", "");
								filterValue = StatUtils.hashString(newSt);
							}

						}

					} else {
						filterValue = Double.parseDouble(con.getValue().toString());
					}

					if (operator.equals("="))
						resultHistogram.equal(filterValue);
					else if (operator.equals(">="))
						resultHistogram.greaterOrEqual(filterValue);
					else if (operator.equals("<="))
						resultHistogram.lessOrEqualValueEstimation(filterValue);
					else if (operator.equals(">"))
						resultHistogram.greaterThan(filterValue);
					else if (operator.equals("<"))
						resultHistogram.lessThanValueEstimation(filterValue);
					// else f = new Filter(col.tableAlias, col.columnName,
					// FilterOperand.NotEqual,
					// Double.parseDouble(con.toString()));

					// adjust RelInfo's histograms based on the resulting
					// histogram
					ni.getResultRel().adjustRelation(col.getName(), resultHistogram);

					// TODO: fix NOdeInfo!!
					ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
					// ni.setTupleLength(ni.getResultRel().getTupleLength());
					// ni.setResultRel(resultRel);
				}
			}
		}

	}*/
	private AttrInfo getAttributeFromBase(String col, Node child, NodeInfo ni) {
		if (child.getChildren().size() == 0 || child.getChildAt(0).getOpCode() != Node.BASEPROJECT) {
			return null;
		}
		AttrInfo column = child.getChildAt(0).getChildAt(0).getNodeInfo().getResultRel().getAttrIndex().get(col);
		return column;
	}
	
	private void applyFilterToNode(Operand nextFilter, NodeInfo ni, Node child) {
		if (nextFilter instanceof UnaryWhereCondition) {
			UnaryWhereCondition uwc = (UnaryWhereCondition) nextFilter;
			if (uwc.getType() == UnaryWhereCondition.LIKE) {
				// for now treat like equality
				// TODO treat properly
				try {
					Column col = uwc.getAllColumnRefs().get(0);
					String con = uwc.getValue();
					if (!ni.getResultRel().getAttrIndex().containsKey(col.getName())) {
						AttrInfo att = getAttributeFromBase(col.toString(), child, ni);
						if (att == null) {
							log.error("Column not found in Attribute index: " + col.toString());
							ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
							return;
						} else {
							ni.getResultRel().getAttrIndex().put(col.toString(), att);
						}
					}
					Histogram resultHistogram = ni.getResultRel().getAttrIndex().get(col.getName()).getHistogram();

					double filterValue = 0;

					filterValue = StatUtils.hashString(con);
					String newSt = "";

					// if (con.startsWith("\'")) {
					newSt = con.replaceAll("\'", "").replaceAll("%", "");
					if (uwc.getOperand().toString().toLowerCase().contains("lower")) {
						newSt = newSt.toUpperCase();
					}
					if (uwc.getOperand().toString().toLowerCase().contains("upper")) {
						newSt = newSt.toLowerCase();
					}
					filterValue = StatUtils.hashString(newSt);
					// }
					log.debug("LIKE operator, removing % :" + newSt);
					resultHistogram.equal(filterValue);

					ni.getResultRel().adjustRelation(col.getName(), resultHistogram);

					// TODO: fix NOdeInfo!!
					ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
				} catch (Exception e) {
					log.error("Could not compute selectivity for filter: " + nextFilter);
				}

			}
			// normally you don't care for these conditions (Column IS NOT
			// NULL)
			// UnaryWhereCondition uwc = (UnaryWhereCondition) nextFilter;
			// Table t=(Table) child.getObject();
			// if(t.getName().startsWith("table")){
			// not base table

			// TODO: fix nodeInfo
			// do nothing!

			// this.planInfo.get(n.getHashId()).setNumberOfTuples(child.getNumberOfTuples());
			// this.planInfo.get(n.getHashId()).setTupleLength(child.getTupleLength());
			// System.out.println(uwc);
		} else if (nextFilter instanceof NonUnaryWhereCondition) {
			// TODO correct this!
			NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) nextFilter;
			// System.out.println(nuwc);
			String operator = nuwc.getOperator(); // e.g. =, <, >

			if (operator.equalsIgnoreCase("and")) {
				applyFilterToNode(nuwc.getLeftOp(), ni, child);
				applyFilterToNode(nuwc.getRightOp(), ni, child);
			} else if (operator.equalsIgnoreCase("or")) {
				log.warn("Filter with OR condition " + nuwc.toString()
						+ ". Selectivity estimation will not be accurate");
				// TODO fix estimation with OR conditions
				// for now just return the one with higher cardinality
				NodeInfo left = new NodeInfo();
				left.setNumberOfTuples(ni.getNumberOfTuples());
				left.setTupleLength(ni.getTupleLength());
				left.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), left, child);

				NodeInfo right = new NodeInfo();
				right.setNumberOfTuples(ni.getNumberOfTuples());
				right.setTupleLength(ni.getTupleLength());
				right.setResultRel(new RelInfo(ni.getResultRel()));
				applyFilterToNode(nuwc.getLeftOp(), right, child);

				ni = left.getNumberOfTuples() > right.getNumberOfTuples() ? left : right;
			} else {

				Column col;
				Constant con;

				if (nuwc.getLeftOp() instanceof Column) {// TODO: constant
					col = (Column) nuwc.getLeftOp();
					con = (Constant) nuwc.getRightOp();
				} else {
					col = (Column) nuwc.getRightOp();
					con = (Constant) nuwc.getLeftOp();
				}

				// RelInfo lRel =
				// this.schema.getTableIndex().get(col.tableAlias);
				// RelInfo lRel = childInfo.getResultRel();
				// RelInfo resultRel = new RelInfo(lRel);
				// RelInfo resultRel = initRel;

				// check to see if it is "cut" by base projection
				
				if (!ni.getResultRel().getAttrIndex().containsKey(col.getName())) {
					AttrInfo att = getAttributeFromBase(col.toString(), child, ni);
					if (att == null) {
						log.error("Column not found in Attribute index: " + col.toString());
						if (!ni.getResultRel().getAttrIndex().containsKey(col.toString())) {
							col.setAlias(null);
						}
						if (!ni.getResultRel().getAttrIndex().containsKey(col.toString())) {
							col.setName(col.getName().replaceAll("\"", ""));
						}
						if (att == null) {
						ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
						return;
						}
					} else {
						ni.getResultRel().getAttrIndex().put(col.toString(), att);
					}

				}
				Histogram resultHistogram = ni.getResultRel().getAttrIndex().get(col.getName()).getHistogram();

				double filterValue = 0;
				if (!con.isArithmetic()) {
					if (con.getValue() instanceof String) {
						String st = (String) con.getValue();
						filterValue = StatUtils.hashString(con.getValue().toString());
						String newSt = "";
						if (st.startsWith("\'")) {
							newSt = st.replaceAll("\'", "");
							filterValue = StatUtils.hashString(newSt);
						}

					}

				} else {
					filterValue = Double.parseDouble(con.getValue().toString());
				}

				if (operator.equals("="))
					resultHistogram.equal(filterValue);
				else if (operator.equals(">="))
					resultHistogram.greaterOrEqual(filterValue);
				else if (operator.equals("<="))
					resultHistogram.lessOrEqualValueEstimation(filterValue);
				else if (operator.equals(">"))
					resultHistogram.greaterThan(filterValue);
				else if (operator.equals("<"))
					resultHistogram.lessThanValueEstimation(filterValue);
				// else f = new Filter(col.tableAlias, col.columnName,
				// FilterOperand.NotEqual, Double.parseDouble(con.toString()));

				// adjust RelInfo's histograms based on the resulting histogram
				ni.getResultRel().adjustRelation(col.getName(), resultHistogram);

				// TODO: fix NOdeInfo!!
				ni.setNumberOfTuples(ni.getResultRel().getNumberOfTuples());
				// ni.setTupleLength(ni.getResultRel().getTupleLength());
				// ni.setResultRel(resultRel);
			}
		}

	}


	public void estimateJoin(Node n, NonUnaryWhereCondition nuwc, Node left, Node right) {
		// NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) n.getObject();
		NodeInfo ni = new NodeInfo();
		Column l = (Column) nuwc.getLeftOp();
		Column r = (Column) nuwc.getRightOp();
		// String equals = nuwc.getOperator();

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = left.getNodeInfo().getResultRel();
		RelInfo rRel = right.getNodeInfo().getResultRel();

		RelInfo resultRel = new RelInfo(lRel);
		RelInfo newR = new RelInfo(rRel);

		Histogram resultHistogram = resultRel.getAttrIndex().get(l.getName()).getHistogram();

		if (newR.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			resultHistogram.join(newR.getAttrIndex().get(r.getName()).getHistogram());
		}

		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(newR.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l.getName(), resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		schema.getTableIndex().put(l.getAlias(), resultRel);
		schema.getTableIndex().put(r.getAlias(), resultRel);

		// adding necessary equivalent hashing attribures
		resultRel.getHashAttr().addAll(newR.getHashAttr());

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		n.setNodeInfo(ni);
	}

	public void estimateFilterJoin(Node n, NonUnaryWhereCondition nuwc) {
		Node child = n.getChildAt(0).getChildAt(0);
		// NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) n.getObject();
		NodeInfo ni = new NodeInfo();
		Column l = (Column) nuwc.getLeftOp();
		Column r = (Column) nuwc.getRightOp();
		// String equals = nuwc.getOperator();

		// RelInfo lRel = this.schema.getTableIndex().get(l.tableAlias);
		// RelInfo rRel = this.schema.getTableIndex().get(r.tableAlias);
		RelInfo lRel = child.getNodeInfo().getResultRel();
		RelInfo rRel = child.getNodeInfo().getResultRel();

		RelInfo resultRel = new RelInfo(lRel);

		Histogram resultHistogram = resultRel.getAttrIndex().get(l.getName()).getHistogram();

		if (rRel.getNumberOfTuples() < 0.5 || lRel.getNumberOfTuples() < 0.5) {
			resultHistogram.convertToTransparentHistogram();
		} else {
			resultHistogram.filterjoin(rRel.getAttrIndex().get(r.getName()).getHistogram());
		}
		// lRel.getAttrIndex().get(l.columnName).getHistogram().join(rRel.getAttrIndex().get(r.columnName).getHistogram());

		// put all the right's RelInfo AttrInfos to the left one
		resultRel.getAttrIndex().putAll(rRel.getAttrIndex());

		// adjust RelInfo's histograms based on the resulting histogram
		resultRel.adjustRelation(l.getName(), resultHistogram);

		// fix alias mappings to RelInfo. The joining aliases must point to the
		// same RelInfo after the join operation
		schema.getTableIndex().put(l.getAlias(), resultRel);
		schema.getTableIndex().put(r.getAlias(), resultRel);

		// adding necessary equivalent hashing attribures
		resultRel.getHashAttr().addAll(rRel.getHashAttr());

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(resultRel.getNumberOfTuples());
		ni.setTupleLength(resultRel.getTupleLength());
		ni.setResultRel(resultRel);
		n.setNodeInfo(ni);
	}

	public void estimateProject(Node n) {
		// String tableAlias;
		NodeInfo ni = new NodeInfo();
		n.setNodeInfo(ni);
		Set<String> columns = new HashSet<String>();
		Node prjNode = n.getChildAt(0);
		Node child = prjNode.getChildAt(0);
		Projection p = (Projection) prjNode.getObject();
		List<Output> outputs = p.getOperands();
		// tableAlias = ((Column)outputs.get(0).getObject()).tableAlias;

		// RelInfo rel = this.schema.getTableIndex().get(tableAlias);
		if (child.getNodeInfo() == null) {
			this.makeEstimationForNode(child);
		}
		RelInfo rel = child.getNodeInfo().getResultRel();

		RelInfo resultRel = new RelInfo(rel);

		for (Output o : outputs) {
			List<Column> cols = o.getObject().getAllColumnRefs();
			if (!cols.isEmpty()) {
				Column c = (Column) o.getObject().getAllColumnRefs().get(0);
				columns.add(c.getName());
			}
		}

		// remove unecessary columns
		resultRel.eliminteRedundantAttributes(columns);

		// TODO: fix nodeInfo
		ni.setNumberOfTuples(child.getNodeInfo().getNumberOfTuples());
		// ni.setTupleLength(child.getNodeInfo().getTupleLength());
		ni.setTupleLength(resultRel.getTupleLength());
		// System.out.println("is this correct?");
		ni.setResultRel(resultRel);
		n.setNodeInfo(ni);
	}

	public void estimateUnion(Node n) {
		Node unionOp = n.getChildAt(0);
		List<Node> children = unionOp.getChildren();
		double numOfTuples = 0;
		double tupleLength = children.get(0).getNodeInfo().getTupleLength();

		for (Node cn : children) {
			numOfTuples += cn.getNodeInfo().getNumberOfTuples();
		}
		NodeInfo ni = new NodeInfo();
		// TODO: fix nodeInfo
		ni.setNumberOfTuples(numOfTuples);
		ni.setTupleLength(tupleLength);
		n.setNodeInfo(ni);
	}

	public void estimateBase(Node n) {
		NodeInfo pi = new NodeInfo();
		String tableAlias = ((Table) n.getObject()).getName();
		RelInfo rel = this.schema.getTableIndex().get(tableAlias);
		if (rel == null) {
			rel = this.schema.getTableIndex().get(tableAlias.toLowerCase());
		}
		// RelInfo rel = this.planInfo.get(n.getHashId()).getResultRel();

		// System.out.println(rel);
		RelInfo resultRel = new RelInfo(rel);

		// TODO: fix nodeInfo
		pi.setNumberOfTuples(rel.getNumberOfTuples());
		pi.setTupleLength(rel.getTupleLength());
		pi.setResultRel(resultRel);
		n.setNodeInfo(pi);
	}

	public double getDuplicateEstimation(String table, Set<String> columns) {
		RelInfo rel = this.schema.getTableIndex().get(table);
		double maxDiff = 1;
		String minFreqCol = "";
		double minfreq = rel.getNumberOfTuples();
		for (String output : columns) {
			try {
				double diff = rel.getAttrIndex().get(output).getHistogram().distinctValues();
				if (diff > maxDiff) {
					maxDiff = diff;

				}
				if (rel.getNumberOfTuples() / diff < minfreq) {
					minfreq = rel.getNumberOfTuples() / diff;
					minFreqCol = output;
				}

			} catch (NullPointerException e) {
				System.out.println("ddd");
			}
		}

		// double no=rel.getNumberOfTuples();
		// double result=1.0;
		if(columns.size()>1){
		for (String output : columns) {
			if (output.equals(minFreqCol)) {
				continue;
			}
			try {
				minfreq = minfreq / rel.getAttrIndex().get(output).getHistogram().distinctValues();
			} catch (NullPointerException e) {
				System.out.println(e.getMessage());
			}
			if (maxDiff < 1.0) {
				return 1.0;
			}

		}
		return minfreq;
		}
		else{
			return rel.getNumberOfTuples() / maxDiff;
		}
		

	}

	/* private-util methods */
	public static double hashString(String str) {
		if (str == null)
			return 0;
		double hashStringVal = 0.0;
		if (str.length() >= HASH_STRING_CHARS) {
			char[] hashChars = new char[HASH_STRING_CHARS];

			for (int i = 0; i < HASH_STRING_CHARS; i++) {
				hashChars[i] = str.charAt(i);
			}

			for (int i = 0; i < HASH_STRING_CHARS; i++) {
				hashStringVal += (double) ((int) hashChars[i])
						* Math.pow((double) HASH_STRING_BASE, (double) (HASH_STRING_CHARS - i));
			}
			return hashStringVal;
		}

		else {
			char[] hashChars = new char[str.length()];

			for (int i = 0; i < str.length(); i++)
				hashChars[i] = str.charAt(i);

			for (int i = 0; i < str.length(); i++) {
				hashStringVal += (double) ((int) hashChars[i])
						* Math.pow((double) HASH_STRING_BASE, (double) (HASH_STRING_CHARS - i));
			}

			return hashStringVal;
		}

	}

	public static double getDuplicateEstimation(NodeInfo r, Set<String> columns) {
		
		
		
		RelInfo rel = r.getResultRel();
		double maxDiff = 1;
		String minFreqCol = "";
		double minfreq = rel.getNumberOfTuples();
		for (String output : columns) {
			if (output.contains(".")){
				String[] splitted = output.split("\\.");
				output=splitted[splitted.length-1];
			}
				
			if (output.contains("`"))
				output = output.replaceAll("`", "");
			if (output.contains("\""))
				output = output.replaceAll("\"", "");
			
			try {
				double diff = rel.getAttrIndex().get(output).getHistogram().distinctValues();
				if (diff > maxDiff) {
					maxDiff = diff;

				}
				if (rel.getNumberOfTuples() / diff < minfreq) {
					minfreq = rel.getNumberOfTuples() / diff;
					minFreqCol = output;
				}

			} catch (NullPointerException e) {
				System.out.println("ERROR....");
				return 1.0;
			}
		}

		// double no=rel.getNumberOfTuples();
		// double result=1.0;
		if(columns.size()>1){
		for (String output : columns) {
			if (output.contains(".")){
				String[] splitted = output.split("\\.");
				output=splitted[splitted.length-1];
			}
				
			if (output.contains("`"))
				output = output.replaceAll("`", "");
			if (output.equals(minFreqCol)) {
				continue;
			}
			try {
				minfreq = minfreq / rel.getAttrIndex().get(output).getHistogram().distinctValues();
			} catch (NullPointerException e) {
				System.out.println(e.getMessage());
			}
			if (minfreq < 1.0) {
				return 1.0;
			}

		}
		return minfreq;
		}
		else{
			return rel.getNumberOfTuples() / maxDiff;
		}
		
	}
		
	
}
