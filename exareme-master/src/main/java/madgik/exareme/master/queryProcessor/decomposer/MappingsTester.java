package madgik.exareme.master.queryProcessor.decomposer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import it.unibz.inf.ontop.exception.InvalidMappingException;
import it.unibz.inf.ontop.exception.InvalidPredicateDeclarationException;
import it.unibz.inf.ontop.io.ModelIOManager;
import it.unibz.inf.ontop.model.*;
import it.unibz.inf.ontop.model.impl.FunctionalTermImpl;
import it.unibz.inf.ontop.model.impl.OBDADataFactoryImpl;
import it.unibz.inf.ontop.model.impl.SQLQueryImpl;
import it.unibz.inf.ontop.model.impl.VariableImpl;
import it.unibz.inf.ontop.owlrefplatform.core.QuestConstants;
import it.unibz.inf.ontop.owlrefplatform.core.QuestPreferences;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWL;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConfiguration;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConnection;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLFactory;
import it.unibz.inf.ontop.r2rml.R2RMLReader;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.decomposer.query.UnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.util.InterfaceAdapter;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.master.queryProcessor.estimator.NodeInfo;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import net.sf.jsqlparser.schema.Table;

public class MappingsTester {

	public static void main(String[] args) throws IOException, InvalidPredicateDeclarationException, InvalidMappingException {
		Map<String, Set<Set<String>>> queryOutputs = new HashMap<String, Set<Set<String>>>();

		/*
		 * OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		 * OWLOntology ontology = null; try { //ontology =
		 * manager.loadOntologyFromOntologyDocument(new File(
		 * "/home/dimitris/workspace/optique/WP09/ontology/well-ontology/2016-05-25/well-ontology-20160525.ttl"
		 * )); ontology = manager.loadOntologyFromOntologyDocument(new File(
		 * "/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.owl"));
		 * //ontology = manager.loadOntologyFromOntologyDocument(new
		 * File("/home/dimitris/ontopv1/npd-benchmark/ontology/npd-v2-ql.owl"));
		 * } catch (OWLOntologyCreationException e1) { // TODO Auto-generated
		 * catch block e1.printStackTrace(); } Set<String> domains=new
		 * HashSet<String>(); Set<String> ranges=new HashSet<String>();
		 * for(OWLObjectProperty op:ontology.getObjectPropertiesInSignature()){
		 * for(OWLObjectPropertyDomainAxiom
		 * domain:ontology.getObjectPropertyDomainAxioms(op)){
		 * if(!domain.getClassesInSignature().isEmpty()){
		 * domains.add(op.getIRI().toString()); break; } }
		 * for(OWLObjectPropertyRangeAxiom
		 * range:ontology.getObjectPropertyRangeAxioms(op)){
		 * if(!range.getClassesInSignature().isEmpty()){
		 * ranges.add(op.getIRI().toString()); break; } } } for(OWLDataProperty
		 * op:ontology.getDataPropertiesInSignature()){
		 * for(OWLDataPropertyDomainAxiom
		 * domain:ontology.getDataPropertyDomainAxioms(op)){
		 * if(!domain.getClassesInSignature().isEmpty()){
		 * domains.add(op.getIRI().toString()); break; } } }
		 * 
		 * //String obdafile=
		 * "/home/dimitris/workspace/optique/WP09/ontology/well-ontology/2016-05-25/well-ontology-20160525.r2rml";
		 * 
		 * String obdafile=
		 * "/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.ttl";
		 * //String obdafile=
		 * "/home/dimitris/ontopv1/npdnew/npd-benchmark/mappings/postgres/ontop>=1.17/npd-v2-ql-postgres-ontop1.17.ttl";
		 * R2RMLReader reader=null; try { reader = new R2RMLReader(obdafile); }
		 * catch (Exception e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } List<OBDAMappingAxiom>
		 * ax=reader.readMappings();
		 * 
		 * for(OBDAMappingAxiom axiom:ax){ String s=null; OBDASQLQuery
		 * sql=axiom.getSourceQuery(); if(sql instanceof SQLQueryImpl){
		 * SQLQueryImpl sqlImpl=(SQLQueryImpl) sql; s=sqlImpl.getSQLQuery(); }
		 * for(Function f:axiom.getTargetQuery()){ Predicate
		 * a=f.getFunctionSymbol(); if(domains.contains(a.getName())){ Term
		 * t=f.getTerm(0); Set<String> outputs=new HashSet<String>(); if(t
		 * instanceof FunctionalTermImpl){ FunctionalTermImpl
		 * fti=(FunctionalTermImpl)t; for(int i=1;i<fti.getTerms().size();i++){
		 * Term term=fti.getTerm(i); if(term instanceof VariableImpl){
		 * VariableImpl v=(VariableImpl)term; outputs.add(v.getName()); } } }
		 * if(!outputs.isEmpty()){ if(queryOutputs.containsKey(s)){
		 * queryOutputs.get(s).add(outputs); } else{ Set<Set<String>>
		 * outputsForquery=new HashSet<Set<String>>();
		 * outputsForquery.add(outputs); queryOutputs.put(s, outputsForquery); }
		 * } }
		 * 
		 * if(ranges.contains(a.getName())){ Term t=f.getTerm(1); Set<String>
		 * outputs=new HashSet<String>(); if(t instanceof FunctionalTermImpl){
		 * FunctionalTermImpl fti=(FunctionalTermImpl)t; for(int
		 * i=1;i<fti.getTerms().size();i++){ Term term=fti.getTerm(i); if(term
		 * instanceof VariableImpl){ VariableImpl v=(VariableImpl)term;
		 * outputs.add(v.getName()); } } } if(!outputs.isEmpty()){
		 * if(queryOutputs.containsKey(s)){ queryOutputs.get(s).add(outputs); }
		 * else{ Set<Set<String>> outputsForquery=new HashSet<Set<String>>();
		 * outputsForquery.add(outputs); queryOutputs.put(s, outputsForquery); }
		 * } }
		 * 
		 * } }
		 */
		QuestOWL reasoner;
		QuestOWLConnection conn;
		 //String
		 //owlfile="/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.owl";
		String owlfile = "/home/dimitris/ontopv1/npd-benchmark/ontology/npd-v2-ql.owl";
		//String owlfile = "/home/dimitris/ontopv1/ontop1.8/ontop/quest-test/src/main/resources/testcases-scenarios/virtual-mode/fishmark/fishdelish.owl";
		// String
		// obdafile="/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.ttl";
		String obdafile = "/home/dimitris/npd-v2-ql-postgres-ontop1.17.ttl";
		//String obdafile ="/home/dimitris/ontopv1/ontop1.8/ontop/quest-test/src/main/resources/testcases-scenarios/virtual-mode/fishmark/fishdelish-mysql.obda";
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = null;
		try {
			ontology = manager.loadOntologyFromOntologyDocument((new File(owlfile)));
		} catch (OWLOntologyCreationException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// Loading the OBDA model

		OBDAModel obdaModel;
		OBDADataFactory fac = OBDADataFactoryImpl.getInstance();

		// if(!this.is_r2rml){
		// obdaModel = fac.getOBDAModel();
		// ModelIOManager ioManager = new ModelIOManager(obdaModel);
		// ioManager.load(obdafile);
		// } else {
		boolean r2rml=true;
		if(r2rml){
		R2RMLReader reader = null;
		try {
			reader = new R2RMLReader(obdafile);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String sourceUrl = obdafile;
		// String jdbc_url="jdbc:mysql://127.0.0.1/lubm100";
		// String dbuser="root";
		// String dbpassword="gray769watt724!@#";
		// String jdbc_driver="com.mysql.jdbc.Driver";
		String jdbc_url = "jdbc:postgresql://127.0.0.1/npd_new_scale500";
		String dbuser = "postgres";
		String dbpassword = "gray769watt724!@#";
		String jdbc_driver = "org.postgresql.Driver";
		OBDADataSource dataSource = fac.getJDBCDataSource(sourceUrl, jdbc_url, dbuser, dbpassword, jdbc_driver);
		obdaModel = reader.readModel(dataSource);
		}
		else{
		obdaModel = fac.getOBDAModel();
	    ModelIOManager ioManager = new ModelIOManager(obdaModel);
	    ioManager.load(obdafile);
		}
	    
		
		// }

		// Preparing the configuration for the new Quest instance, we need to
		// make sure it will be setup for "virtual ABox mode"
		QuestPreferences p = new QuestPreferences();
		p.setCurrentValueOf(QuestPreferences.ABOX_MODE, QuestConstants.VIRTUAL);
		p.setCurrentValueOf(QuestPreferences.OBTAIN_FULL_METADATA, QuestConstants.FALSE);
		p.setCurrentValueOf(QuestPreferences.SQL_GENERATE_REPLACE, QuestConstants.TRUE);
		p.setCurrentValueOf(QuestPreferences.REWRITE, QuestConstants.TRUE);
		// p.setCurrentValueOf(QuestPreferences.DISTINCT_RESULTSET,
		// QuestConstants.TRUE);
		p.setCurrentValueOf(QuestPreferences.REFORMULATION_TECHNIQUE, QuestConstants.TW);

		// Creating the instance of the reasoner using the factory. Remember
		// that the RDBMS that contains the data must be already running and
		// accepting connections.
		QuestOWLConfiguration.Builder configBuilder = QuestOWLConfiguration.builder();
		configBuilder.obdaModel(obdaModel);
		configBuilder.preferences(p);
		QuestOWLConfiguration config = configBuilder.build();
		QuestOWLFactory factory = new QuestOWLFactory();
		// QuestOWLConfiguration config =
		// QuestOWLConfiguration.builder().obdaModel(obdaModel).preferences(p).build();
		reasoner = factory.createReasoner(ontology, config);

		Map<String, Set<Set<String>>> tMapSQL = reasoner.getTMappingsSQL();

		NodeSelectivityEstimator nse = null;
		Map<String, Set<ViewInfo>> viewinfos = new HashMap<String, Set<ViewInfo>>();
		int no=0;
		int all=0;
		for (String sql : tMapSQL.keySet()) {
			NodeHashValues hashes = new NodeHashValues();

			try {
				// nse = new
				// NodeSelectivityEstimator("/home/dimitris/histograms-replaced.json");
				// nse = new
				 //NodeSelectivityEstimator("/media/dimitris/T/exaremelubm100/"
				 //+ "histograms.json");
				nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremenpd100new2/" + "histograms.json");
				//nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremefish/" + "histograms.json");
			} catch (Exception e) {

			}
			hashes.setSelectivityEstimator(nse);
			try {
				SQLQuery query = SQLQueryParser.parse(sql, hashes);
				boolean onlySingleTables = false;
				if (onlySingleTables) {
					if (query.getInputTables().size() != 1 || !query.getGroupBy().isEmpty()
							|| !query.getOrderBy().isEmpty()) {
						continue;
					}
					String table = query.getInputTables().get(0).getName().toLowerCase();

					for (Set<String> outputs : queryOutputs.get(sql)) {
						double dupl = nse.getDuplicateEstimation(table, outputs);
						if (dupl > 1.100002) {
							no++;
							System.out.println("no"+no);
							String viewName = table + Util.createUniqueId();
							String colame = outputs.iterator().next();
							System.out.println("from sql:" + sql);
							System.out.println("table:" + table + " columns:" + outputs + " dupl. est." + dupl);
							System.out.println("view sql: create table " + viewName + " as select distinct " + colame
									+ " from " + table + " " + query.getWhereSQL());

							Set<ViewInfo> viewsForTable = null;
							if (viewinfos.containsKey(table)) {
								viewsForTable = viewinfos.get(table);
							} else {
								viewsForTable = new HashSet<ViewInfo>();
								viewinfos.put(table, viewsForTable);
							}
							ViewInfo vi = new ViewInfo(viewName, outputs);
							viewsForTable.add(vi);
							for (NonUnaryWhereCondition o : query.getBinaryWhereConditions()) {
								vi.addCondition(o);
							}
							for (UnaryWhereCondition o : query.getUnaryWhereConditions()) {
								vi.addCondition(o);
							}
						}

						Gson gson = new GsonBuilder()
								.registerTypeAdapter(Operand.class, new InterfaceAdapter<Operand>()).create();
						java.lang.reflect.Type viewType = new TypeToken<Map<String, Set<ViewInfo>>>() {
						}.getType();
						String jsonStr = gson.toJson(viewinfos, viewType);

						PrintWriter writer = new PrintWriter("/tmp/" + "views.json", "UTF-8");
						writer.println(jsonStr);
						writer.close();
					}
				} else {
					if (!query.getGroupBy().isEmpty() || !query.getOrderBy().isEmpty()) {
						continue;
					}

					QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
					d.setN2a(new NamesToAliases());
					// String table="avavav";
					String table = query.getInputTables().get(0).getName().toLowerCase();
					NodeInfo r = d.getSelectivityForTopNode();

					// renameOutputs(queryOutputs.get(sql), query);
					for (Set<String> outputs : tMapSQL.get(sql)) {
						all++;
						System.out.println(all);
						double dupl = NodeSelectivityEstimator.getDuplicateEstimation(r, outputs);
						if (dupl > 1.000002) {
							
							no++;
							System.out.println("no"+no);
							String viewName = table + Util.createUniqueId();
							// String
							// colame=outputs.toString();//.iterator().next();
							System.out.println("from sql:" + sql);
							System.out.println("table:" + table + " columns:" + outputs + " dupl. est." + dupl);
							String outs = "";
							String del = "";
							if (outputs.isEmpty()) {
								outs += " 1 as x ";
							}
							for (String o : outputs) {
								outs += del;
								outs += o;
								del = ", ";
							}
							String inputs = "";
							del = "";
							for (madgik.exareme.master.queryProcessor.decomposer.query.Table t : query
									.getInputTables()) {
								inputs += del;
								inputs += t.toString();
								del = ", ";
							}

							String limit = "";
							if (outputs.isEmpty()) {

								limit = " limit 1 ";
							}
							System.out.println("view sql: create table " + viewName + " as select distinct " + outs
									+ " from " + inputs + " " + query.getWhereSQL()+ limit);

							Set<ViewInfo> viewsForTable = null;
							if (viewinfos.containsKey(table)) {
								viewsForTable = viewinfos.get(table);
							} else {
								viewsForTable = new HashSet<ViewInfo>();
								viewinfos.put(table, viewsForTable);
							}
							ViewInfo vi = new ViewInfo(viewName, outputs);
							viewsForTable.add(vi);
							for (NonUnaryWhereCondition o : query.getBinaryWhereConditions()) {
								vi.addCondition(o);
							}
							for (UnaryWhereCondition o : query.getUnaryWhereConditions()) {
								vi.addCondition(o);
							}
						}

						Gson gson = new GsonBuilder()
								.registerTypeAdapter(Operand.class, new InterfaceAdapter<Operand>()).create();
						java.lang.reflect.Type viewType = new TypeToken<Map<String, Set<ViewInfo>>>() {
						}.getType();
						String jsonStr = gson.toJson(viewinfos, viewType);

						PrintWriter writer = new PrintWriter("/tmp/" + "views.json", "UTF-8");
						writer.println(jsonStr);
						writer.close();
					}
				}

			} catch (Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace(System.out);
			}
		}

		System.out.println("sss");

	}

	private static void renameOutputs(Set<Set<String>> outputs, SQLQuery query) {
		Map<Set<String>, Set<String>> toRepl = new HashMap<Set<String>, Set<String>>();
		for (Set<String> outs : outputs) {
			Set<String> replace = new HashSet<String>();
			toRepl.put(outs, replace);
			for (String out : outs) {
				for (Output o : query.getOutputs()) {
					if (out.equals(o.getOutputName())) {
						if (o.getObject() instanceof Column) {
							Column c = (Column) o.getObject();
							replace.add(c.getName());
							break;
						} else {
							System.out.println("ERROR!!!");

						}
					}
				}
			}

		}
		for (Set<String> original : toRepl.keySet()) {
			outputs.remove(original);
			outputs.add(toRepl.get(original));
		}
	}

}
