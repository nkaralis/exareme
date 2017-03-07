/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * @author dimitris
 */
public class NonUnaryWhereCondition implements Operand {

	private List<Operand> ops;
	// private Operand leftOp;
	// private Operand rightOp;
	String operator;

	public NonUnaryWhereCondition() {
		super();
		ops = new ArrayList<Operand>();
	}

	public NonUnaryWhereCondition(Operand left, Operand right, String operator) {
		ops = new ArrayList<Operand>();
		this.ops.add(left);
		this.ops.add(right);
		this.operator = operator;
	}

	public NonUnaryWhereCondition(List<Operand> operands, String operator) {
		this.ops = operands;
		this.operator = operator;
	}

	@Override
	public List<Column> getAllColumnRefs() {
		List<Column> res = new ArrayList<Column>();
		for (Operand o : this.ops) {
			for (Column c : o.getAllColumnRefs()) {
				res.add(c);
			}
		}
		return res;
	}

	public void setOperator(String op) {
		this.operator = op;
	}

	public String getOperator() {
		return this.operator;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (operator.equalsIgnoreCase("or")) {
			sb.append("(");
		}
		sb.append(this.ops.get(0).toString());

		sb.append(" ");
		sb.append(operator);
		sb.append(" ");
		sb.append(ops.get(1).toString());
		for (int i = 2; i < this.ops.size(); i++) {
			// multiway join
			sb.append(" AND ");
			sb.append(ops.get(i - 1).toString());
			sb.append(operator);
			sb.append(" ");
			sb.append(ops.get(i).toString());
		}
		if (operator.equalsIgnoreCase("or")) {
			sb.append(")");
		}
		return sb.toString();
	}

	public List<Operand> getOperands() {
		return this.ops;
	}

	public Operand getLeftOp() {
		return this.ops.get(0);
	}

	public Operand getRightOp() {
		return this.ops.get(1);
	}

	public Operand getOp(int i) {
		return this.ops.get(i);
	}

	public void setLeftOp(Operand op) {
		if (this.ops.isEmpty()) {
			this.ops.add(op);
		} else {
			this.ops.set(0, op);
		}
	}

	public void setRightOp(Operand op) {
		if (this.ops.isEmpty()) {
			this.setLeftOp(new Column("temp", "temp"));
			this.ops.add(op);
		} else if (this.ops.size() == 1) {
			this.ops.add(op);
		} else {
			this.ops.set(1, op);
		}
	}

	@Override
	public NonUnaryWhereCondition clone() throws CloneNotSupportedException {
		NonUnaryWhereCondition cloned = (NonUnaryWhereCondition) super.clone();
		List<Operand> opsCloned = new ArrayList<Operand>();
		for (Operand o : this.ops) {
			opsCloned.add(o.clone());
		}
		cloned.ops = opsCloned;
		return cloned;
	}

	public void setOperandAt(int i, Operand op) {
		this.ops.set(i, op);
	}

	public void addOperand(Operand op) {
		this.ops.add(op);
	}

	@Override
	public int hashCode() {
		boolean unorder = this.operator.toLowerCase().equals("and") || this.operator.toLowerCase().equals("or");
		int hash = 3;
		for (Operand o : this.ops) {
			if (unorder || this.operator.equals("=")) {
				// in join commutativity does not affect the result

				hash += (o != null ? o.hashCode() : 0);
			} else {
				// int test=this.ops.hashCode();
				hash = hash * 43 + (o != null ? o.hashCode() : 0);
			}
		}
		hash = hash * 43 + (this.operator != null ? this.operator.hashCode() : 0);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final NonUnaryWhereCondition other = (NonUnaryWhereCondition) obj;
		boolean unorder = this.operator.toLowerCase().equals("and") || this.operator.toLowerCase().equals("or");

		if (unorder) {
			Set<Operand> thisOps = new HashSet<Operand>();
			this.getUnorderedOperands(thisOps);//new HashSet<Operand>(this.ops);
			Set<Operand> otherOps =new HashSet<Operand>();
			other.getUnorderedOperands(otherOps);//new HashSet<Operand>(other.ops);
			if (this.ops != other.ops && !thisOps.equals(otherOps)) {
				return false;
			}
		}
		else if (this.operator.equals("=") && this.getAllColumnRefs().size() == 1) {
			Set<Operand> thisOps = new HashSet<>(this.ops);
			Set<Operand> otherOps =new HashSet<>(other.ops);
			if (this.ops != other.ops && !thisOps.equals(otherOps)) {
				return false;
			}
		}
		else {
			if (this.ops != other.ops && (this.ops == null || !this.ops.equals(other.ops))) {
				return false;
			}
		}
		if ((this.operator == null) ? (other.operator != null) : !this.operator.equals(other.operator)) {
			return false;
		}
		return true;
	}

	@Override
	public void changeColumn(Column oldCol, Column newCol) {
		for (int i = 0; i < this.ops.size(); i++) {
			Operand o = this.ops.get(i);
			if (o.getClass().equals(Column.class)) {
				if (((Column) o).equals(oldCol)) {
					this.ops.set(i, newCol);
				}
			} else {
				o.changeColumn(oldCol, newCol);
			}
		}
	}

	@Override
	public HashCode getHashID() {
		List<HashCode> codes = new ArrayList<HashCode>();
		for (Operand o : this.ops) {
			codes.add(o.getHashID());
		}
		codes.add(Hashing.sha1().hashBytes(operator.toUpperCase().getBytes()));

		return Hashing.combineOrdered(codes);

	}

	public boolean referencesAtMostOneTable() {
		List<Column> cols = this.getAllColumnRefs();
		if (cols.isEmpty()) {
			return true;
		}
		String tableName = cols.get(0).getAlias();
		for (int i = 1; i < cols.size(); i++) {
			if (!tableName.equals(cols.get(i).getAlias())) {
				return false;
			}
		}
		return true;
	}

	public Set<NonUnaryWhereCondition> getOrConditions() {
		Set<NonUnaryWhereCondition> result = new HashSet<NonUnaryWhereCondition>();
		if (this.operator.equals("=")) {
			if ((this.getLeftOp() instanceof Column || this.getLeftOp() instanceof Constant)
					&& (this.getRightOp() instanceof Column || this.getRightOp() instanceof Constant)) {
				result.add(this);
				return result;
			} else {
				return null;
			}
		} else if (this.operator.equalsIgnoreCase("OR")
				&& (this.getLeftOp() instanceof NonUnaryWhereCondition || this.getLeftOp() instanceof BinaryOperand)
				&& (this.getRightOp() instanceof NonUnaryWhereCondition)
				|| this.getRightOp() instanceof BinaryOperand) {
			Set<NonUnaryWhereCondition> left = null;
			if (this.getLeftOp() instanceof NonUnaryWhereCondition) {
				left = ((NonUnaryWhereCondition) this.getLeftOp()).getOrConditions();
			} else if (this.getLeftOp() instanceof BinaryOperand) {
				left = ((BinaryOperand) this.getLeftOp()).getOrConditions();
			}
			if (left == null)
				return null;
			Set<NonUnaryWhereCondition> right = null;
			if (this.getRightOp() instanceof NonUnaryWhereCondition) {
				right = ((NonUnaryWhereCondition) this.getRightOp()).getOrConditions();
			} else if (this.getLeftOp() instanceof BinaryOperand) {
				right = ((BinaryOperand) this.getRightOp()).getOrConditions();
			}
			if (right == null)
				return null;
			result.addAll(left);
			result.addAll(right);
			return result;
		} else {
			return null;
		}

	}

	private void getUnorderedOperands(Set<Operand> ops) {
		boolean unordered = this.operator.toLowerCase().equals("or");// ||this.operator.toLowerCase().equals("and");
		if (unordered) {
			ops.add(this.getLeftOp());
			//Set<Operand> right = new HashSet<Operand>();
			if (this.getRightOp() instanceof NonUnaryWhereCondition) {
				NonUnaryWhereCondition nuwc = (NonUnaryWhereCondition) this.getRightOp();
				nuwc.getUnorderedOperands(ops);
			}
			if (this.getRightOp() instanceof BinaryOperand) {
				BinaryOperand bo = (BinaryOperand) this.getRightOp();
				bo.getUnorderedOperands(ops);
			}
		} else {
			ops.add(this);
		}
	}

}
