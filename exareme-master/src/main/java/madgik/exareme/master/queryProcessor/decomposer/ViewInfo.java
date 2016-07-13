package madgik.exareme.master.queryProcessor.decomposer;

import java.util.HashSet;
import java.util.Set;

import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.UnaryWhereCondition;

public class ViewInfo {

	private String outColumn;
	private Set<UnaryWhereCondition> unaryConditions;
	private Set<NonUnaryWhereCondition> binaryConditions;
	private String viewName;
	private boolean or;

	public ViewInfo(String name, String outColumn) {
		super();
		this.viewName = name;
		this.outColumn = outColumn;
		unaryConditions = new HashSet<UnaryWhereCondition>();
		binaryConditions = new HashSet<NonUnaryWhereCondition>();
		or=false;
	}

	public boolean addCondition(Object toAdd) {
		if (toAdd instanceof NonUnaryWhereCondition)
			return binaryConditions.add((NonUnaryWhereCondition) toAdd);
		else if (toAdd instanceof UnaryWhereCondition)
			return unaryConditions.add((UnaryWhereCondition) toAdd);
		else
			return false;
	}


	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((binaryConditions == null) ? 0 : binaryConditions.hashCode());
		result = prime * result + ((outColumn == null) ? 0 : outColumn.hashCode());
		result = prime * result + ((unaryConditions == null) ? 0 : unaryConditions.hashCode());
		result = prime * result + ((viewName == null) ? 0 : viewName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ViewInfo other = (ViewInfo) obj;
		if (binaryConditions == null) {
			if (other.binaryConditions != null)
				return false;
		} else if (!binaryConditions.equals(other.binaryConditions))
			return false;
		if (outColumn == null) {
			if (other.outColumn != null)
				return false;
		} else if (!outColumn.equals(other.outColumn))
			return false;
		if (unaryConditions == null) {
			if (other.unaryConditions != null)
				return false;
		} else if (!unaryConditions.equals(other.unaryConditions))
			return false;
		if (viewName == null) {
			if (other.viewName != null)
				return false;
		} else if (!viewName.equals(other.viewName))
			return false;
		return true;
	}

	public boolean containsCondition(Object o) {
		if (o instanceof NonUnaryWhereCondition)
			return binaryConditions.contains(o);
		else if (o instanceof UnaryWhereCondition)
			return unaryConditions.contains(o);
		else
			return false;
	}

	public String getOutput() {
		return this.outColumn;
	}

	public String getTableName() {
		return this.viewName;
	}

	public int getNumberOfConditions() {
		if(this.or){
			return 1;
		}
		else{
			return unaryConditions.size()+binaryConditions.size();
		}
	}

	public void addConditions(ViewInfo other) {
		this.binaryConditions.addAll(other.binaryConditions);
		this.unaryConditions.addAll(other.unaryConditions);
		
	}

	public boolean isOr() {
		return or;
	}

	public void setOr(boolean or) {
		this.or = or;
	}

	public boolean orsAreEqual(Set<NonUnaryWhereCondition> ors) {
		return ors.equals(this.binaryConditions);
	}
	
	

}
