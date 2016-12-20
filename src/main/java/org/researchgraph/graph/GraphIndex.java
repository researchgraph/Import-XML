package org.researchgraph.graph;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GraphIndex {
	private final String label;
	private final String property;
	
	public GraphIndex(String label) {
		this.label = label;
		this. property = GraphUtils.PROPERTY_KEY;
	}

	public GraphIndex(String label, String  property) {
		this.label = label;
		this. property =  property;
	}
	
	public String getLabel() {
		return label;
	}
	
	public String getProperty() {
		return  property;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(label)
				.append(property)
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
		
		GraphIndex other = (GraphIndex) obj;
		return new EqualsBuilder()
                .append(label, other.label)
                .append(property, other.property)
                .isEquals();
	}

	@Override
	public String toString() {
		return "GraphKey [label=" + label + ", property=" + property + "]";
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private String label;
		private String property = GraphUtils.PROPERTY_KEY;
		
		public Builder withLabel(String label) {
			this.label = label;
			return this;
		}
		
		public Builder withProperty(String property) {
			this.property = property;
			return this;
		}
		
		public GraphIndex build() {
			return new GraphIndex(label, property); 
		}
	}
}
