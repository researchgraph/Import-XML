package org.researchgraph.graph;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * 
 * @author dima
 *
 */

public class GraphNode extends GraphProperties {
	private GraphKey key;
	private final Set<String> labels = new HashSet<String>();
	private final Map<GraphIndex, Object> indexes = new HashMap<GraphIndex, Object>();
	
	public GraphNode(GraphKey key, Set<String> labels, 
			Map<GraphIndex, Object> indexes, Map<String, Object> properties) {
		super(properties);
		
		this.key = key;
		if (null != labels) {
			this.labels.addAll(labels); 
		}
		if (null != indexes) {
			this.indexes.putAll(indexes);
		}
	}
	
	public boolean hasKey() {
		return null != key;
	}
	
	public GraphKey getKey() {
		return key;
	}
	
	public void setKey(GraphKey key) {
		this.key = key;
	}

	public void setKey(GraphIndex index, Object value) {
		this.key = new GraphKey(index, value);
	}
	
	public void setKey(String index, Object value) {
		setKey(new GraphIndex(index), value);
	}

	public void setKey(String index, String key, Object value) {
		setKey(new GraphIndex(index, key), value);
	}

	public Map<GraphIndex, Object> getIndexes() {
		return indexes;
	}
	
	public Set<GraphKey> getIndexSet() {
		return null == indexes ? null : indexes
				.entrySet()
				.stream()
				.map(e -> new GraphKey(e.getKey(), e.getValue()))
				.collect(Collectors.toSet());
	}
	
	public void setIndex(GraphIndex index, Object value) {
		indexes.put(index, value);
	}
	
	public void setIndex(String index, Object value) {
		setIndex(new GraphIndex(index), value);
	}

	public void setIndex(String index, String key, Object value) {
		setIndex(new GraphIndex(index, key), value);
	}

	public boolean hasNodeSource() {
		return hasProperty(GraphUtils.PROPERTY_SOURCE);
	}
	
	public Object getNodeSource() {
		return getProperty(GraphUtils.PROPERTY_SOURCE);
	}
	
	public void setNodeSource(Object source) {
		setProperty(GraphUtils.PROPERTY_SOURCE, source);
	}

	public void addNodeSource(Object source) {
		addProperty(GraphUtils.PROPERTY_SOURCE, source);
	}

	public boolean hasNodeType() {
		return hasProperty(GraphUtils.PROPERTY_TYPE);
	}
	
	public Object getNodeType() {
		return getProperty(GraphUtils.PROPERTY_TYPE);
	}
	
	public void setNodeType(Object type) {
		setProperty(GraphUtils.PROPERTY_TYPE, type);
	}
	
	public Set<String> getLabels() {
		return labels;
	}
	
	public void addLabels(Set<String> labels) {
		this.labels.addAll(labels);
	}
	
	public void addLabel(String label) {
		labels.add(label);
	}

	public boolean isDeleted() {
		Object deleted = getProperty(GraphUtils.PROPERTY_DELETED);
		return null != deleted && (Boolean) deleted;
	}

	public boolean isBroken() {
		Object broken = getProperty(GraphUtils.PROPERTY_BROKEN);
		return null != broken && (Boolean) broken;
	}
	
	public void setDeleted(boolean deleted) {
		setProperty(GraphUtils.PROPERTY_DELETED, deleted);
	}

	public void setBroken(boolean broken) {
		setProperty(GraphUtils.PROPERTY_BROKEN, broken);
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private GraphKey key;
		private final Set<String> labels = new HashSet<>();
		private final Map<GraphIndex, Object> indexes = new HashMap<>();
		private final Map<String, Object> properties = new HashMap<>();

	
		public Builder withKey(GraphKey key) {
			this.key = key;
			return this;
		}
		
		public Builder withKey(GraphIndex key, Object value) {
			return withKey(new GraphKey(key, value));
		}
	
		public Builder withKey(String index, Object value) {
			return withKey(new GraphIndex(index), value);
		}

		public Builder withKey(String index, String property, Object value) {
			return withKey(new GraphIndex(index, property), value);
		}
	
		public Builder withIndex(GraphIndex index, Object value) {
			indexes.put(index, value);
			return this;
		}
		
		public Builder withIndex(GraphKey key) {
			return withIndex(key.getIndex(), key.getValue());
		}
	
		public Builder withIndex(String index, Object value) {
			return withIndex(new GraphIndex(index), value);
		}

		public Builder withIndex(String index, String property, Object value) {
			return withIndex(new GraphIndex(index, property), value);
		}
	
		public Builder withProperties(Map<String, Object> properties) {
			this.properties.putAll(properties);
			return this;
		}
	
		public Builder withProperty(String property, Object value) {
			this.properties.put(property, value);
			return this;
		}
	
		public Builder withNodeSource(String source) {
			return withProperty(GraphUtils.PROPERTY_SOURCE, source);
		}
	
		public Builder withNodeType(String type) {
			return withProperty(GraphUtils.PROPERTY_TYPE, type);
		}
		
		public Builder withLabels(Set<String> labels) {
			this.labels.addAll(labels);
			return this;
		}
		
		public Builder withLabel(String label) {
			labels.add(label);
			return this;
		}
		
		public Builder withDeleted(boolean deleted) {
			return withProperty(GraphUtils.PROPERTY_DELETED, deleted);
		}
	
		public Builder withBroken(boolean broken) {
			return withProperty(GraphUtils.PROPERTY_BROKEN, broken);
		}
		
		public GraphNode build() {
			return new GraphNode(key, labels, indexes, properties);
		}
	}
	
	@Override
	public String toString() {
		return "GraphNode [key=" + key + ", labels=" + labels + ", indexes=" + indexes + ", properties=" + properties + "]";
	}
}
