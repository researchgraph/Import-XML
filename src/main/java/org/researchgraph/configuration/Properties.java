package org.researchgraph.configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Properties {
	public static final Path CONFIG_FILE = Paths.get("import.conf");
	
	public static final String PROPERTY_NEO4J_FOLDER = "neo4j";
	public static final String PROPERTY_S3_BUCKET = "s3.bucket";
	public static final String PROPERTY_S3_PREIFX = "s3.prefix";
	public static final String PROPERTY_XML_FOLDER = "xml.folder";
	public static final String PROPERTY_XML_TYPE = "xml.type";
	public static final String PROPERTY_SOURCE = "source";
	public static final String PROPERTY_CROSSWALK = "crosswalk";
	public static final String PROPERTY_VERSIONS_FOLDER = "versions.folder";
	public static final String PROPERTY_CONFIG_FILE = "config-file";
	
	public static final String PROPERTY_HELP = "help";
	
	public static final String DEFAULT_NEO4J_FOLDER = "neo4j";
	public static final String DEFAULT_VERSIONS_FOLDER = "versions";
	public static final String DEFAULT_XML_TYPE = "rg";
	
	public static Configuration fromArgs(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		
		// create the Options
		Options options = new Options();
		options.addOption( "n", PROPERTY_NEO4J_FOLDER, true, "Neo4J folder" );
		options.addOption( "b", PROPERTY_S3_BUCKET, true, "S3 Bucket" );
		options.addOption( "p", PROPERTY_S3_PREIFX, true, "S3 Prefix" );
		options.addOption( "f", PROPERTY_XML_FOLDER, true, "XML Folder" );
		options.addOption( "t", PROPERTY_XML_TYPE, true, "XML Type" );
		options.addOption( "s", PROPERTY_SOURCE, true, "Source name" );
		options.addOption( "C", PROPERTY_CROSSWALK, true, "Crosswalk" );
		options.addOption( "c", PROPERTY_CONFIG_FILE, true, "configuration file (optional)" );
		options.addOption( "v", PROPERTY_VERSIONS_FOLDER, true, "versions folder" );
		options.addOption( "h", PROPERTY_HELP, false, "print this message" );

		// parse the command line arguments
		CommandLine line = parser.parse( options, args );

		if (line.hasOption( PROPERTY_HELP )) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "java -jar neo4j-importer-[verion].jar [PARAMETERS]", options );
            
            System.exit(0);
		}
		
		// variables to store program properties
		CompositeConfiguration config = new CompositeConfiguration();
		
		BaseConfiguration defaultConfig = new BaseConfiguration();
		defaultConfig.setProperty( PROPERTY_NEO4J_FOLDER, DEFAULT_NEO4J_FOLDER );
		defaultConfig.setProperty( PROPERTY_VERSIONS_FOLDER, DEFAULT_VERSIONS_FOLDER );
		defaultConfig.setProperty( PROPERTY_XML_TYPE, DEFAULT_XML_TYPE );
		
		BaseConfiguration commandLineConfig = new BaseConfiguration();
		
		Path configurationFile = null;
		
		for (Option option : line.getOptions()) {
			if ( PROPERTY_CONFIG_FILE.equals(option.getLongOpt()) ) {
				configurationFile = Paths.get(option.getValue());
			} else {
				commandLineConfig.setProperty(option.getLongOpt(), option.getValue());
			}
		}
			
		if (null == configurationFile) {
			configurationFile = CONFIG_FILE;
		}
		
		if (Files.isRegularFile( configurationFile ) && Files.isReadable( configurationFile )) {
			config.addConfiguration(new PropertiesConfiguration( configurationFile.toFile() ));
		} else {
			if (CONFIG_FILE != configurationFile) {
				throw new Exception("Invalid configuration file: " + configurationFile.toString());
			}
		}
		
		config.addConfiguration(commandLineConfig);
		config.addConfiguration(defaultConfig);
			
		// the program has default output file, but input file must be presented
		if ( !config.containsKey( PROPERTY_NEO4J_FOLDER ) )
			throw new Exception("Please specify Neo4J folder file");

		// the program has default output file, but input file must be presented
		if ( !config.containsKey( PROPERTY_SOURCE ) )
			throw new Exception("Please provide Source Name");
		
		// the program has default output file, but input file must be presented
		if ( !config.containsKey( PROPERTY_VERSIONS_FOLDER ) )
			throw new Exception("Please provide Versions Folder");
				 
		return config;
	}
}