# Importer-XML
This program enables importing the XML files into Neo4j using Research Graph Schema.




## Notes for compiling the program


To compile with Java 8, create a file named `jaxp.properties` (if it doesn't exist) under /path/to/jdk1.8.0/jre/lib and then write this line in it:

```
javax.xml.accessExternalSchema = all
```

or just execute 

```
mvn clean package -Djavax.xml.accessExternalSchema=all
```


## Release notes

### Version 1.3.6
[GitHub.com/ResearchGraph/Import-XML/releases/tag/v1.3.6](https://github.com/researchgraph/Import-XML/releases/tag/v1.3.6)

- **Profiling option**: A new option that shows the time spent by individual functions. Note this function can create an excessive output content in the terminal; hence, we only recommend to use this option for the testing purpose.
- **Unknown Relation Keys Log**: New capability for creating a log file from the **relation** keys that cannot be fund in any nodes during the import process.
