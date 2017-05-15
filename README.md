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
