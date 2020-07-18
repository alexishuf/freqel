package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.rdf.model.Property;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public abstract class ImplementationByProperties extends Implementation {
    private final @Nonnull List<Node_URI> requiredProperties;

    public ImplementationByProperties(@Nonnull Property... requiredProperties) {
        assert requiredProperties.length > 0;
        this.requiredProperties = new ArrayList<>(requiredProperties.length);
        for (Property property : requiredProperties)
            this.requiredProperties.add((Node_URI) property.asNode());
    }

    @Override
    public boolean canWrap(Node node, EnhGraph eg) {
        Graph g = eg.asGraph();
        for (Node_URI property : requiredProperties) {
            if (!g.contains(node, property, null)) return false;
        }
        return true;
    }
}
