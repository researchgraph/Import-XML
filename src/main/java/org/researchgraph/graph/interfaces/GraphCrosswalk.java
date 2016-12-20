package org.researchgraph.graph.interfaces;

import java.io.InputStream;

import javax.xml.bind.JAXBException;

import org.researchgraph.graph.Graph;

public interface GraphCrosswalk {
	void setSource(String source);
	String getSource();
	Graph process(InputStream xml) throws Exception;
}
