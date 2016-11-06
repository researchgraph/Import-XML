package org.researchgraph.graph;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GraphKey  {
	private final GraphIndex index;
	private final Object value;

	public GraphKey(GraphIndex index, Object value) {
		this.index = index;
		this.value = value;
	}
	 	
	public GraphKey(String label, Object value) {
		this.index = new GraphIndex(label);
		this.value = value;
	}

	public GraphKey(String label, String property, Object value) {
		this.index = new GraphIndex(label, property);
		this.value = value;
	}
	
	public GraphIndex getIndex() {
		return index;
	}
	
	public String getLabel() {
		return null == index ? null : index.getLabel();
	}
	
	public String getProperty() {
		return null == index ? null : index.getProperty();
	}

	public Object getValue() {
		return value;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(index)
				.append(value)
				.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (null == obj) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (getClass() != obj.getClass()) {
		     return false;
		}
		
		GraphKey other = (GraphKey) obj;
		return new EqualsBuilder()
                .append(index, other.index)
                .append(value, other.value)
                .isEquals();
	}
	
	@Override
	public String toString() {
		return "GraphKey [index=" + index + ", value=" + value + "]";
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private GraphIndex index;
		private Object value;
		
		public Builder withIndex(GraphIndex index) {
			this.index = index;
			return this;
		}
		
		public Builder withValue(Object value) {
			this.value = value;
			return this;
		}

		public GraphKey build() {
			return new GraphKey(index, value); 
		}
	}
}
