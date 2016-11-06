package org.researchgraph.graph;

import java.util.HashMap;
import java.util.Map;

/**
 * A class to store single Relationship between two nodes
 * 
 * It consists from:
 *  - relationship: a type of relationship
 *  - start: a set of properties, describing start node
 *  - end: a set of properties, describing end node 
 *  - this: an optional set of relationship properties.
 *  
 * Typically, nodes properties consists from two fields:
 *  - source: name of the source and used to distinguish different subsets
 *  - key: unique key of the node within source. If source does not exists,
 *         the key must be unique within a database.  
 * 
 * @author Dima Kudriavcev (dmitrij@kudriavcev.info)
 * @date 2015-07-24
 * @version 1.0.0
 */

public class GraphRelationship extends GraphProperties {
    private final String relationship;
    private final GraphKey start;
    private final GraphKey end;
    
    public GraphRelationship(String relationship, GraphKey start, GraphKey end) {
    	this.relationship = relationship;
    	this.start = start;
    	this.end = end;
    }
    
    public GraphRelationship(String relationship, GraphKey start, GraphKey end, Map<String, Object> properties) {
    	super(properties);
    	
    	this.relationship = relationship;
    	this.start = start;
    	this.end = end;
    }

	public String getRelationship() {
		return relationship;
	}

	public GraphKey getStart() {
		return start;
	}
	
	public GraphKey getEnd() {
		return end;
	}

	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private String relationship;
		private GraphKey start;
		private GraphKey end;
		private final Map<String, Object> properties = new HashMap<>();
			
		public Builder withRelationship(String relationship) {
			this.relationship = relationship;
			return this;
		}
		
		public Builder withProperties(Map<String, Object> properties) {
			this.properties.putAll(properties);
			
			return this;
		}
		
		public Builder withProperty(String property, Object value) {
			this.properties.put(property, value);
			
			return this;
		}
		
		public Builder withStart(GraphKey start) {
			this.start = start;
			return this;
		}
	
		public Builder withStart(String index, Object value) {
			this.start = new GraphKey(index, value);
			return this;
		}
	
		public Builder withStart(String index, String property, Object value) {
			this.start = new GraphKey(index, property, value);
			return this;
		}
	
		public Builder withEnd(GraphKey end) {
			this.end = end;
			return this;
		}
	
		public Builder withEnd(String index, Object value) {
			this.end = new GraphKey(index, value);
			return this;
		}
	
		public Builder withEnd(String index, String property, Object value) {
			this.end = new GraphKey(index, property, value);
			return this;
		}
		
		public GraphRelationship build() {
			return new GraphRelationship(relationship, start, end, properties);
		}
	}
	
	@Override
	public String toString() {
		return "GraphRelationship [relationship=" + relationship + ", start="
				+ start + ", end=" + end + ", properties=" + properties + "]";
	}
}
