package org.researchgraph.neo4j;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphIndex;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphSchema;
import org.researchgraph.graph.interfaces.GraphImporter;
import org.researchgraph.neo4j.interfaces.ProcessNode;

public class Neo4jDatabase implements GraphImporter {
	private static final String COLUMN_N = "n";
	private static final String NEO4J_CONF = "/conf/neo4j.conf";
	private static final String NEO4J_DB = "/data/databases/graph.db";
		
	private GraphDatabaseService graphDb;
	
	//private Map<String, Index<Node>> indexes = new HashMap<String, Index<Node>>();
	
	private boolean verbose = false;
	private long nodesCreated = 0;
	private long nodesUpdated = 0;
	private long relationshipsCreated = 0;
	private long relationshipsUpdated = 0;
	
	private final Map<String, List<GraphRelationship>> unknownRelationships = new HashMap<String, List<GraphRelationship>>();
	private final Set<GraphSchema> importedSchemas = new HashSet<GraphSchema>();	
		
	private static File GetDbPath(final String folder) throws Neo4jException, IOException
	{
		File db = new File(folder, NEO4J_DB);
		if (!db.exists())
			db.mkdirs();
				
		if (!db.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return db;
	}
	
	private static File GetConfPath(final String folder) throws Neo4jException
	{
		File conf = new File(folder, NEO4J_CONF);
		if (!conf.exists() || conf.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return conf;
	}	
	
	private static GraphDatabaseService getReadOnlyGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "true" )
				.newGraphDatabase();
			
			registerShutdownHook( graphDb );
			
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	
	private static GraphDatabaseService getGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.newGraphDatabase();
		
			registerShutdownHook( graphDb );
		
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    });
	}
	

	public Neo4jDatabase(final String neo4jFolder) throws Exception {		
		graphDb = getGraphDb( neo4jFolder );
	}

	
	public Neo4jDatabase(final String neo4jFolder, boolean readOnly) throws Exception {
		graphDb = readOnly 
				? getReadOnlyGraphDb(neo4jFolder) 
				: getGraphDb( neo4jFolder );
	}
	
	public GraphDatabaseService getGraphDatabaseService() {
		return graphDb;
	}
		
	public boolean isVerbose() {
		return verbose;
	}

	public long getNodesCreated() {
		return nodesCreated;
	}

	public long getNodesUpdated() {
		return nodesUpdated;
	}

	public long getRelationshipsCreated() {
		return relationshipsCreated;
	}

	public long getRelationshipsUpdated() {
		return relationshipsUpdated;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public void resetCounters() {
		nodesCreated = nodesUpdated = relationshipsCreated = relationshipsUpdated = 0;
	}
	
	public void printStatistics(PrintStream out) {
		out.println(String.format("%d nodes have been created." +
                        "\n%d nodes have been updated." +
                        "\n%d relationships have been created." +
                        "\n%d relationships have been updated." +
                        "\n%d relationship keys are unknown in this graph.",
				nodesCreated, nodesUpdated, relationshipsCreated, relationshipsUpdated, unknownRelationships.size()));
        if (unknownRelationships.size()>0) {
            String logFileAddress ="log_unknown_relations.txt";
            File file= new File(logFileAddress);
            out.println("Please find the list of the unknown relationship keys at: " + file.getAbsolutePath());
            logUnknownRelationships("log_unknown_relations.txt");
        }
    }

    private void logUnknownRelationships(String logFileAddress) {
            try {
                FileWriter writer = new FileWriter(logFileAddress);
                for (Map.Entry<String, List<GraphRelationship>> entry : unknownRelationships.entrySet()) {
                    writer.write(entry.getKey().toString() + "\n");
                }
                writer.flush();
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

	
	public long getSourcesConnectionsCount(String source1, String source2) {
		try ( Transaction ignored = graphDb.beginTx() ) 
		{
			String cypher = "MATCH (n1:" + source1 + ")-[x]-(n2:" + source2 + ") RETURN COUNT (DISTINCT x) AS n";
			try (Result result = graphDb.execute(cypher)) {
				if  ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        return (Long) row.get(COLUMN_N);
			    }
			}
		}
		
		return 0;
	}
	
	
	public void enumrateAllNodes(ProcessNode processNode) throws Exception {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			ResourceIterable<Node> nodes = graphDb.getAllNodes();
			for (Node node : nodes) {
				if (!processNode.processNode(node))
					break;
			}
		
			tx.success();
		}
	}
	
	public void enumrateAllNodesWithLabel(Label label, ProcessNode processNode) throws Exception {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			
			try (ResourceIterator<Node> nodes = graphDb.findNodes(label)) {
				while (nodes.hasNext()) {
					if (!processNode.processNode(nodes.next()))
						break;
				}
			}
			
			tx.success();
		}
	}
	
	public void enumrateAllNodesWithLabel(String label, ProcessNode processNode) throws Exception {
		enumrateAllNodesWithLabel(Label.label(label), processNode);
	}
	
	public void enumrateAllNodesWithProperty(String property, ProcessNode processNode) throws Exception {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			String cypher = "MATCH (n) WHERE HAS(n." + property + ") RETURN n";
			try (Result result = graphDb.execute(cypher)) {
				while ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        if (!processNode.processNode((Node) row.get(COLUMN_N)))
			        	break;
			    }
			}
			
			tx.success();
		}
	}
	
	public void enumrateAllNodesWithLabelAndProperty(String label, String property, ProcessNode processNode)  throws Exception {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			String cypher = "MATCH (n:" + label + ") WHERE HAS(n." + property + ") RETURN n";
			try (Result result = graphDb.execute(cypher)) {
				while ( result.hasNext() )
			    {
			        Map<String,Object> row = result.next();
			        if (!processNode.processNode((Node) row.get(COLUMN_N)))
			        	break;
			    }
			}
			
			tx.success();
		}
	}
	
	public void enumrateAllNodesWithLabelAndProperty(Label label, String property, ProcessNode processNode)  throws Exception {
		enumrateAllNodesWithLabelAndProperty(label.toString(), property, processNode);
	}
	
	public ConstraintDefinition createConstrant(Label label, String key) {
		ConstraintDefinition def = null;
		
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			def = _createConstrant(label, key);
			
			tx.success();
		}
		
		return def;
	}	
	
	public ConstraintDefinition createConstrant(String label, String key) {
		return createConstrant(Label.label(label), key);
	}
	
	public IndexDefinition createIndex(Label label, String key) {
		IndexDefinition def = null;
		
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			def = _createIndex(label, key);
			
			tx.success();
		}
		
		return def;
	}
	
	public IndexDefinition createIndex(String label, String key) {
		return createIndex(Label.label(label), key);
	}

    public void importGraph(Graph graph) {
        importGraph(graph,false);
    }

	public void importGraph(Graph graph, Boolean profilingEnabled) {


		// schema can not be imported in the same transaction as nodes and relationships
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchemas(graph.getSchemas());
			
			tx.success();
		}
		
		try ( Transaction tx = graphDb.beginTx() ) 
		{

            Long minorMarkTime=System.currentTimeMillis(); //for performance profiling
            Long deltaTime =new Long(0);
			_importNodes(graph.getNodes());
            if (profilingEnabled) {
                deltaTime = System.currentTimeMillis() - minorMarkTime;
                System.out.println("_importNodes in milliseconds:" + deltaTime);
            }
            minorMarkTime=System.currentTimeMillis(); //for performance profiling
			_importRelationships(graph.getRelationships(), true);
            if (profilingEnabled) {
                deltaTime = System.currentTimeMillis() - minorMarkTime;
                System.out.println("_importRelationships in milliseconds:" + deltaTime);
            }
			
			tx.success();
		}
	}
	
	public void importSchemas(Collection<GraphSchema> schemas) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchemas(schemas);
		
			tx.success();
		}
	}

	public void importSchema(GraphSchema schema) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importSchema(schema);
		
			tx.success();
		}
	}
	
	public void importNodes(Collection<GraphNode> nodes) {
		// Import nodes
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importNodes(nodes);		
				
			tx.success();
		}
	}

	public void importNode(GraphNode node) {
		// Import nodes
		try ( Transaction tx = graphDb.beginTx() ) 
		{
			_importNode(node);		
				
			tx.success();
		}
	}
	
	public void importRelationships(Collection<GraphRelationship> relationships) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{		
			_importRelationships(relationships, true);
			
			tx.success();
		}
	}
	
	public void importRelationship(GraphRelationship relationship) {
		try ( Transaction tx = graphDb.beginTx() ) 
		{		
			_importRelationship(relationship, true);
			
			tx.success();
		}
	}
	
	private ConstraintDefinition _createConstrant(Label label, String key) {
		Schema schema = graphDb.schema();
		
		for (ConstraintDefinition constraint : schema.getConstraints(label))
			for (String property : constraint.getPropertyKeys())
				if (property.equals(key))
					return constraint;  // already existing
			
		return schema
				.constraintFor(label)
				.assertPropertyIsUnique(key)
				.create();
	}
	
	private ConstraintDefinition _createConstrant(String label, String key) {
		return _createConstrant(Label.label(label), key);
	}
	
	private ConstraintDefinition _createConstrant(GraphIndex index) {
		return _createConstrant(index.getLabel(), index.getProperty());
	}
	
	private IndexDefinition _createIndex(Label label, String key) {
		Schema schema = graphDb.schema();
		
		for (IndexDefinition index : schema.getIndexes(label))
			for (String property : index.getPropertyKeys())
				if (property.equals(key))
					return index;  // already existing
			
		return schema
				.indexFor(label)
				.on(key)
				.create();
	}
	
	private IndexDefinition _createIndex(String label, String key) {
		return _createIndex(Label.label(label), key);
	}
	
	private IndexDefinition _createIndex(GraphIndex index) {
		return _createIndex(index.getLabel(), index.getProperty());
	}
	
	private Node _findAnyNode(Label label, String key, Object value) {
		try (ResourceIterator<Node> nodes = graphDb.findNodes(label, key, value)) {
			if (!nodes.hasNext())
				return null;
			
			return nodes.next();
		}		
	}
	
	private Node _findAnyNode(String label, String key, Object value) {
		return _findAnyNode(Label.label(label), key, value); 		
	}
	
	public Node _findAnyNode(GraphKey key) {
		return _findAnyNode(key.getLabel(), key.getProperty(), key.getValue()); 		
	}	
	
	public List<Node> _findAllNodes(Label label, String key, Object value) {
		try (ResourceIterator<Node> hits = graphDb.findNodes(label, key, value)) {
			List<Node> nodes = new ArrayList<Node>();
			
			while (hits.hasNext()) {
				nodes.add(hits.next());
			}
			
			return nodes;
		}
	}
	
	public List<Node> _findAllNodes(String label, String key, Object value) {
		return _findAllNodes(Label.label(label), key, value);
	}
	
	private List<Node> _findAllNodes(GraphKey key) {
		return _findAllNodes(key.getLabel(), key.getProperty(), key.getValue());
	}
	
	private Relationship _findRelationship(Iterable<Relationship> rels, long nodeId, Direction direction) {
		for (Relationship rel : rels) {
			switch (direction) {
			case INCOMING:
				if (rel.getStartNode().getId() == nodeId)
					return rel;
				break;
			case OUTGOING:
				if (rel.getEndNode().getId() == nodeId)
					return rel;
				
			case BOTH:
				if (rel.getStartNode().getId() == nodeId || 
				    rel.getEndNode().getId() == nodeId)
					return rel;
			}
		}
		
		return null;
	}
	
	private Relationship _findRelationship(Node nodeStart, long nodeId, 
			RelationshipType type, Direction direction) {
		return _findRelationship(nodeStart.getRelationships(type, direction), nodeId, direction);
	}
	
	private Relationship _findRelationship(Node nodeStart, Node endNode, 
			RelationshipType type, Direction direction) {
		return _findRelationship(nodeStart, endNode.getId(), type, direction);
	}
	
	private Node _createNode() {
		++nodesCreated;
		
		return graphDb.createNode();
	}

	private Relationship _createRelationship(Node nodeStart, Node nodeEnd, RelationshipType type) {
		++relationshipsCreated;
		
		return nodeStart.createRelationshipTo(nodeEnd, type);		
	}

	private void _importSchemas(Collection<GraphSchema> schemas) {
		if (null != schemas)
			for (GraphSchema schema : schemas) 
				_importSchema(schema);
	}
	
	private void _importSchema(GraphSchema schema) {
		// make sure we had imported each schema only once
		if (!importedSchemas.contains(schema)) {
			GraphIndex index = schema.getIndex();
			
			if (schema.isUnique()) {
				if (verbose) {
					System.out.println("Creating Constraint {index=" + index + "}");
				}
				_createConstrant(index);
			} else {
				if (verbose) {
					System.out.println("Creating Index {index=" + index + "}");
				}
	
				_createIndex(index);
			}
			
			importedSchemas.add(schema);
		}
	}

	private void _importNodes(Collection<GraphNode> nodes) {
		// Import nodes
		if (null != nodes)
			for (GraphNode graphNode : nodes) 
				_importNode(graphNode);		
	}
	
	private void _importIndex(Node node, GraphKey key) {
		node.addLabel(Label.label(key.getIndex().getLabel()));
		node.setProperty(key.getIndex().getProperty(), key.getValue());
		
		_importRelationships(unknownRelationships.remove(getRelationshipKey(key)), false); 
	}
	
	private void _importIndexes(Node node, Collection<GraphKey> indexes) {
		if (null != indexes) {
			indexes.stream().forEach(i -> _importIndex(node, i));
		}
	}
	
	private void _importLabels(Node node, Collection<String> labels) {
		if (null != labels) {
			labels.stream().map(l -> Label.label(l)).forEach(l -> node.addLabel(l));
		}
	}

	private void _importProperties(Node node, Map<String, Object> properties) {
		if (null != properties) {
			properties.entrySet().stream().forEach(e -> node.setProperty(e.getKey(), e.getValue()));
		}
	}
	
	private void _importProperties(Relationship relationship, Map<String, Object> properties) {
		if (null != properties) {
			properties.entrySet().stream().forEach(e -> relationship.setProperty(e.getKey(), e.getValue()));
		}
	}
	
	public void _importRelationships(Collection<GraphRelationship> relationships, boolean storeUnknown) {
		if (null != relationships) {
			relationships.stream().forEach(r -> _importRelationship(r, storeUnknown));
		}
	}
	
	private Node _importNode(GraphNode graphNode) {
        if (graphNode.isBroken() || graphNode.isDeleted())
            return null;

        GraphKey key = graphNode.getKey();

		/*
		if (StringUtils.isEmpty(key.getLabel()))
			throw new IllegalArgumentException("Node Key Label can not be empty");
		if (StringUtils.isEmpty(key.getProperty()))
            throw new IllegalArgumentException("Node Key Property can not be null");
        if (null == key.getValue())
			throw new IllegalArgumentException("Node Key Value can not be null");
			
		if (verbose) {
			System.out.println("Importing Node (" + key + ")");
		}
		
		Node node = _findAnyNode(key);*/
        Node node;
        //if (null == node) {
        try {
            node = _createNode();

            _importIndex(node, key);
            _importIndexes(node, graphNode.getIndexSet());


            _importLabels(node, graphNode.getLabels());
            _importProperties(node, graphNode.getProperties());

        } catch (Exception e) {
            return null;
        }
		//} else  {
		//	++nodesUpdated;
		//}

		return node;
	}
		
	private void _importRelationship(GraphRelationship graphRelationship, boolean storeUnknown) {
		String relationshipName = graphRelationship.getRelationship();
		GraphKey start = graphRelationship.getStart();
		GraphKey end = graphRelationship.getEnd();
		
		List<Node> nodesStart = _findAllNodes(start);
		if (nodesStart.isEmpty() && storeUnknown) { 
			storeUnknownRelationship(getRelationshipKey(start), graphRelationship);
			
			if (verbose)
				System.out.println("Relationship Start Key (" + start + ") does not exists");
		}
		
		List<Node> nodesEnd = _findAllNodes(end);
		if (nodesEnd.isEmpty() && storeUnknown) {
			storeUnknownRelationship(getRelationshipKey(end), graphRelationship);
			
			if (verbose)
				System.out.println("Relationship End Key (" + end + ") does not exists");
		}
		
		if (nodesStart.isEmpty() || nodesEnd.isEmpty())
			return;
		
		if (verbose) 
			System.out.println("Importing Relationship (" + start + ")-[" + relationshipName + "]->(" + end + ")");
		
		RelationshipType relationshipType = RelationshipType.withName(relationshipName);
		nodesStart.stream().forEach(nodeStart -> {
			nodesEnd.stream().forEach(nodeEnd -> _mergeRelationship(nodeStart, nodeEnd, relationshipType, 
					Direction.OUTGOING, graphRelationship.getProperties()));
		});
	}
	
	private Relationship _mergeRelationship(Node nodeStart, Node nodeEnd, RelationshipType type, 
			Direction direction, Map<String, Object> properties) {

		Relationship relationship = _findRelationship(nodeStart, nodeEnd, type, direction);
		if (null == relationship) 
			relationship = _createRelationship(nodeStart, nodeEnd, type);
		else 
			++relationshipsUpdated;
		
		_importProperties(relationship, properties);
		
		return relationship;
	}
	
	private static String getRelationshipKey(GraphKey key) {
		return key.getLabel() + "." + key.getProperty() + "." + key.getValue();
	}
	
	private void storeUnknownRelationship(String key, GraphRelationship relationship) {
		List<GraphRelationship> list = unknownRelationships.get(key);
		if (null == list) 
			unknownRelationships.put(key, list = new ArrayList<GraphRelationship>());
		
		list.add(relationship);
	}
}
