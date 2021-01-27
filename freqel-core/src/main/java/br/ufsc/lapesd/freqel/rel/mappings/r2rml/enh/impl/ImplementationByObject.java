package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.rdf.model.Property;

import javax.annotation.Nonnull;

public abstract class ImplementationByObject extends Implementation {
    private final @Nonnull Node_URI property;

    public ImplementationByObject(@Nonnull Property property) {
        this.property = (Node_URI) property.asNode();
    }

    @Override
    public boolean canWrap(Node node, EnhGraph eg) {
        return eg.asGraph().contains(null, property, node);
    }
}
