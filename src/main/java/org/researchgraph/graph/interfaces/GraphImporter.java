package org.researchgraph.graph.interfaces;

import java.util.Collection;

import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphSchema;

public interface GraphImporter {
	void importGraph(Graph graph);
	
	void importNode(GraphNode node);
	void importNodes(Collection<GraphNode> nodes);
	
	void importSchema(GraphSchema schema);
	void importSchemas(Collection<GraphSchema> schemas); 
	
	void importRelationship(GraphRelationship relationship);
	void importRelationships(Collection<GraphRelationship> relationships);
	
	boolean isVerbose();
	void setVerbose(boolean verbose);
}
