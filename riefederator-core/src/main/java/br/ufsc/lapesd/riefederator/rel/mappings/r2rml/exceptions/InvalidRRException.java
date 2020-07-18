package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.lang.String.format;

public class InvalidRRException extends RRException {
    private final @Nullable Resource subject;
    private final @Nullable Property property;
    private final @Nonnull String issue;

    public InvalidRRException(@Nonnull Resource subject, @Nonnull Property property,
                              @Nonnull String issue) {
        super(String.format("%s for property %s of subject %s", issue, RR.toString(property), subject));
        this.subject = subject;
        this.property = property;
        this.issue = issue;
    }

    public InvalidRRException(@Nonnull Node node, @Nonnull Graph graph,
                          @Nonnull Class<? extends Resource> aClass) {
        this(node, graph, aClass, "");
    }

    public InvalidRRException(@Nonnull Node node, @Nonnull Graph graph,
                              @Nonnull Class<? extends Resource> aClass,
                              @Nonnull String reason) {
        super(format("Node %s does not represent a %s.%s Node appears as subject in %d triples " +
                     "and as object in %d triples", node, aClass.getSimpleName(),
                     reason.isEmpty() || reason.endsWith(".") ? reason : reason + ".",
                     graph.find(node, null, null).toList().size(),
                     graph.find(null, null, node).toList().size()));
        this.subject = node.isURI() || node.isBlank() ?  new ResourceImpl(node, (EnhGraph) null)
                                                      : null;
        this.property = null;
        this.issue = format("Node %s does not represent a %s", node, aClass.getSimpleName());
    }

    public @Nullable Resource getSubject() {
        return subject;
    }
    public @Nullable Property getProperty() {
        return property;
    }
    public @Nonnull String getIssue() {
        return issue;
    }
}
