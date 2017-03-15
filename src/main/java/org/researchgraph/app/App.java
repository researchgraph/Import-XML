package org.researchgraph.app;
	   
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.*;


import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.researchgraph.configuration.Properties;
import org.researchgraph.crosswalk.CrosswalkRG;
import org.researchgraph.graph.Graph;
import org.researchgraph.neo4j.Neo4jDatabase;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App {
	
	public static void main(String[] args) {
		try {
			Configuration properties = Properties.fromArgs(args);
			        
	        String neo4jFolder = properties.getString(Properties.PROPERTY_NEO4J_FOLDER);
	        
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        
	        System.out.println("Neo4J: " + neo4jFolder);
	        
	        String bucket = properties.getString(Properties.PROPERTY_S3_BUCKET);
	        String prefix = properties.getString(Properties.PROPERTY_S3_PREIFX);
	        String xmlFolder = properties.getString(Properties.PROPERTY_XML_FOLDER);
	        String xmlType = properties.getString(Properties.PROPERTY_XML_TYPE);
	        String source = properties.getString(Properties.PROPERTY_SOURCE);
	        String crosswalk = properties.getString(Properties.PROPERTY_CROSSWALK);
			Boolean verbose = Boolean.parseBoolean( properties.getString(Properties.PROPERTY_VERBOSE));

			Templates template = null;


	        if (!StringUtils.isEmpty(crosswalk)) {
	        	System.out.println("Crosswalk: " + crosswalk);

				TransformerFactory tfactory = net.sf.saxon.TransformerFactoryImpl.newInstance();

				SAXTransformerFactory stfactory = (SAXTransformerFactory) tfactory;

				template = tfactory.newTemplates(new StreamSource(crosswalk));

	        } 
	        
	        CrosswalkRG.XmlType type = CrosswalkRG.XmlType.valueOf(xmlType); 
	        
	        if (!StringUtils.isEmpty(bucket) && !StringUtils.isEmpty(prefix)) {
	        	System.out.println("S3 Bucket: " + bucket);
	        	System.out.println("S3 Prefix: " + prefix);
				System.out.println("Version folder: " + properties.getString(Properties.PROPERTY_VERSIONS_FOLDER));
				System.out.println("Vernbose: " +  verbose.toString());


	        	String versionFolder = properties.getString(Properties.PROPERTY_VERSIONS_FOLDER);
		        if (StringUtils.isEmpty(versionFolder))
		            throw new IllegalArgumentException("Versions Folder can not be empty");
	        	
	        	processS3Files(bucket, prefix, neo4jFolder, versionFolder, source, type, template, verbose);
	        } else if (!StringUtils.isEmpty(xmlFolder)) {
	        	System.out.println("XML: " + xmlFolder);
	        	
	        	processFiles(xmlFolder, neo4jFolder, source, type, template,verbose);
	        } else
                throw new IllegalArgumentException("Please provide either S3 Bucket and prefix OR a path to a XML Folder");

	        	        
	       // debugFile(accessKey, secretKey, bucket, "rda/rif/class:collection/54800.xml");
	        
        	
		} catch (Exception e) {
            e.printStackTrace();
            
            System.exit(1);
		}       
	}
	
	private static void processS3Files(String bucket, String prefix, String neo4jFolder,
									   String versionFolder, String source, CrosswalkRG.XmlType type,
									   Templates template, Boolean verbose) throws Exception {
        AmazonS3 s3client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
        
        CrosswalkRG crosswalk = new CrosswalkRG();
        crosswalk.setSource(source);
        crosswalk.setType(type);
		crosswalk.setVerbose(verbose);
        
    	Neo4jDatabase neo4j = new Neo4jDatabase(neo4jFolder);
    	neo4j.setVerbose(verbose);
    		    
    	ListObjectsRequest listObjectsRequest;
		ObjectListing objectListing;
		
		String file = prefix + "/latest.txt";
		S3Object object = s3client.getObject(new GetObjectRequest(bucket, file));
		
		String latest;
		try (InputStream txt = object.getObjectContent()) {
			latest = IOUtils.toString(txt, StandardCharsets.UTF_8).trim();
		}
		
		if (StringUtils.isEmpty(latest)) 
			throw new Exception("Unable to find latest harvest in the S3 Bucket (latest.txt file is empty or not avaliable). Please check if you have access to S3 bucket and did you have completed the harvestring.");	
		
		String folder = prefix + "/" + latest + "/";
		
		System.out.println("S3 Repository: " + latest);
		
	    listObjectsRequest = new ListObjectsRequest()
			.withBucketName(bucket)
			.withPrefix(folder);
	    do {
			objectListing = s3client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				
				file = objectSummary.getKey();

				Long markTime = System.currentTimeMillis();
		        System.out.print("Processing file: " + file);
				
				object = s3client.getObject(new GetObjectRequest(bucket, file));
				
				if (null != template) {
					Source reader = new StreamSource(object.getObjectContent());
					StringWriter writer = new StringWriter();
					
					Transformer transformer = template.newTransformer(); 
					transformer.transform(reader, new StreamResult(writer));
					
					InputStream stream = new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8));
					
					Graph graph = crosswalk.process(stream);
					neo4j.importGraph(graph);
		        } else {
		        	InputStream xml = object.getObjectContent();
						
		        	Graph graph = crosswalk.process(xml);
					neo4j.importGraph(graph);
				}
				Long deltaTime= markTime == 0 ? 0 : (System.currentTimeMillis() - markTime)/1000;
				System.out.println(", completed in seconds:" + deltaTime);
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());
	    
	    Files.write(Paths.get(versionFolder, source), latest.getBytes());
	    
		System.out.println("Done");
				
		crosswalk.printStatistics(System.out);
		neo4j.printStatistics(System.out);
		
		
	}
	
	private static void processFiles(String xmlFolder, String neo4jFolder, String source, 
			CrosswalkRG.XmlType type, Templates template, Boolean verbose) throws Exception {
		CrosswalkRG crosswalk = new CrosswalkRG();
        crosswalk.setSource(source);
        crosswalk.setType(type);
     	crosswalk.setVerbose(verbose);
        
    	Neo4jDatabase neo4j = new Neo4jDatabase(neo4jFolder);
		neo4j.setVerbose(Boolean.parseBoolean(Properties.PROPERTY_VERBOSE));
		neo4j.setVerbose(verbose);
    	//importer.setVerbose(true);
    		    
		File[] files = new File(xmlFolder).listFiles();
		for (File file : files) 
			if (!file.isDirectory()) 
		        try (InputStream xml = new FileInputStream(file))
		        {

					Long markTime = System.currentTimeMillis();
			        System.out.print("Processing file: " + file);
			        
			        if (null != template) {
						Source reader = new StreamSource(xml);
						StringWriter writer = new StringWriter();
						
						Transformer transformer = template.newTransformer(); 
						transformer.transform(reader, new StreamResult(writer));
						
						InputStream stream = new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8));
						
						Graph graph = crosswalk.process(stream);
						neo4j.importGraph(graph);

			        } else {
						Graph graph = crosswalk.process(xml);
						neo4j.importGraph(graph);
					}

					Long deltaTime= markTime == 0 ? 0 : (System.currentTimeMillis() - markTime)/1000;
					System.out.println(", completed in seconds:" + deltaTime);
		        }
		
		System.out.println("Done");
		
		crosswalk.printStatistics(System.out);
		neo4j.printStatistics(System.out);
	}

}
