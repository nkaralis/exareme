/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package madgik.exareme.master.queryProcessor.decomposer.query;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * @author dimitris
 */
public class Constant implements Operand {

    private Object value;
    private boolean isArithmetic;

    public Object getValue() {
        return value;
    }

    public Constant() {
        super();
        this.value = new Object();
        this.isArithmetic = false;
    }

    public Constant(Object constant) {
        this.value = constant;
        this.isArithmetic = false;
    }

    @Override public String toString() {
        return value.toString();
    }



    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isArithmetic ? 1231 : 1237);
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		Constant other = (Constant) obj;
		if (isArithmetic != other.isArithmetic)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public void setValue(Object v) {
        this.value = v;
    }

    @Override public List<Column> getAllColumnRefs() {
        return new ArrayList<Column>();
    }

    @Override public void changeColumn(Column oldCol, Column newCol) {
    }

    @Override public Constant clone() throws CloneNotSupportedException {
        Constant cloned = (Constant) super.clone();
        return cloned;
    }

    void setArithmetic(boolean b) {
        this.isArithmetic = true;
    }

    public boolean isArithmetic() {
        return isArithmetic;
    }

	@Override
	public HashCode getHashID() {
		return Hashing.sha1().hashBytes(this.toString().toUpperCase().getBytes());
	}

}
