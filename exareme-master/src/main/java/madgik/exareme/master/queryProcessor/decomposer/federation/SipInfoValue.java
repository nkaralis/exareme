package madgik.exareme.master.queryProcessor.decomposer.federation;

import java.util.List;

import madgik.exareme.master.queryProcessor.decomposer.dag.Node;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;

public class SipInfoValue {
	
	private Node node;
	private List<Column> outputs;
	
	public SipInfoValue(Node node, List<Column> outputs) {
		super();
		this.node = node;
		this.outputs = outputs;
	}

	public Node getNode() {
		return node;
	}

	public List<Column> getOutputs() {
		return outputs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((node == null) ? 0 : node.hashCode());
		result = prime * result + ((outputs == null) ? 0 : outputs.hashCode());
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
		SipInfoValue other = (SipInfoValue) obj;
		if (node.getObject() == null) {
			if (other.node.getObject() != null)
				return false;
		} else if (!node.getObject().equals(other.node.getObject()))
			return false;
		if (outputs == null) {
			if (other.outputs != null)
				return false;
		} else if (!outputs.equals(other.outputs))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SipInfoValue [node=" + node + "]";
	}
	
	
	
	
	
	

}
