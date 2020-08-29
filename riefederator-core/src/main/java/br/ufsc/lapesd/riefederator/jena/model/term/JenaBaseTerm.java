package br.ufsc.lapesd.riefederator.jena.model.term;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

public interface JenaBaseTerm {
    @Nonnull Node getGraphNode();
    @Nonnull RDFNode getModelNode();
}
