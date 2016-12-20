package org.researchgraph.graph;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


/**
 * A class to store schema of the Graph
 * 
 * Typically it has a label used to distinguish a Node by it source or type 
 * and an index name used to create index or constraint by that name.
 * Additional unique flag can be set to indicate what this index must be unique within a label.
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-24
 * @version 1.0.0
 */

public class GraphSchema {
	private final GraphIndex index;
	private final boolean unique;
	
	public GraphSchema(GraphIndex index, boolean unique) {
		this.index = index;
		this.unique = unique;
	}

	public GraphSchema(String label, boolean unique) {
		this.index = new GraphIndex(label);
		this.unique = unique;
	}

	public GraphSchema(String label, String property, boolean unique) {
		this.index = new GraphIndex(label, property);
		this.unique = unique;
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
	 
	public boolean isUnique() {
		return unique;
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
		
		GraphSchema other = (GraphSchema) obj;
		return new EqualsBuilder()
                .append(index, other.index)
                .append(unique, other.unique)
                .isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(index)
				.append(unique)
				.toHashCode();
	}

	@Override
	public String toString() {
		return "GraphSchema [index=" + index + ", unique="
				+ unique + "]";
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private GraphIndex index;
		private boolean unique;
		
		public Builder withIndex(GraphIndex index) {
			this.index = index;
			return this;
		}
		
		public Builder withUnique(boolean unique) {
			this.unique = unique;
			return this;
		}
		
		public GraphSchema build() {
			return new GraphSchema(index, unique);
		}
	}
}
