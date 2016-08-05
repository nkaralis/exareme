package madgik.exareme.master.queryProcessor.decomposer.federation;

import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;

public class SipJoin {
	
	private int joinNumber;
	private NonUnaryWhereCondition join;
	private String sipName;
	boolean deleteOnTableInsert;
	
	
	
	public SipJoin(int joinNumber, NonUnaryWhereCondition join, String sipName, boolean d) {
		super();
		this.joinNumber = joinNumber;
		this.join = join;
		this.sipName = sipName;
		this.deleteOnTableInsert=d;
	}



	public String getSipName() {
		return sipName;
	}



	public boolean isDeleteOnTableInsert() {
		return deleteOnTableInsert;
	}



	@Override
	public String toString() {
		return "SipJoin [joinNumber=" + joinNumber + ", join=" + join
				+ ", sipName=" + sipName + ", deleteOnTableInsert="
				+ deleteOnTableInsert + "]";
	}



	public NonUnaryWhereCondition getBwc() {
		return join;
	}



	public int getNumber() {
		return this.joinNumber;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (deleteOnTableInsert ? 1231 : 1237);
		result = prime * result + ((join == null) ? 0 : join.hashCode());
		result = prime * result + joinNumber;
		result = prime * result + ((sipName == null) ? 0 : sipName.hashCode());
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
		SipJoin other = (SipJoin) obj;
		if (deleteOnTableInsert != other.deleteOnTableInsert)
			return false;
		if (join == null) {
			if (other.join != null)
				return false;
		} else if (!join.equals(other.join))
			return false;
		if (joinNumber != other.joinNumber)
			return false;
		if (sipName == null) {
			if (other.sipName != null)
				return false;
		} else if (!sipName.equals(other.sipName))
			return false;
		return true;
	}
	
	

}
