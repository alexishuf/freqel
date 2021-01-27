package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RRTemplate;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.vocabulary.RDF;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class TermMapImpl extends ResourceImpl implements TermMap {
    protected TermMapImpl(Node n, EnhGraph m, @Nonnull Class<? extends TermMap> anInterface) {
        super(n, m);
        checkValidNonShortcut(n, m.asGraph(), anInterface);
    }
    protected TermMapImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    protected void checkValidNonShortcut(@Nonnull Node n, @Nonnull Graph graph,
                                         @Nonnull Class<? extends TermMap> anInterface) {
        boolean ok = graph.contains(n, RR.column.asNode(), null)
                || graph.contains(n, RR.template.asNode(), null)
                || graph.contains(n, RR.constant.asNode(), null)
                || graph.contains(n, RR.parentTriplesMap.asNode(), null);
        if (!ok) {
            throw new InvalidRRException(node, graph, anInterface, "Node has no rr:column, " +
                                         "rr:template, rr:constant nor rr:parentTriplesMap");
        }
    }

    @Override
    public @Nullable RDFNode getConstant() {
        return RRUtils.getNode(this, RR.constant);
    }

    @Override
    public @Nullable String getColumn() {
        return RRUtils.getString(this, RR.column);
    }

    @Override
    public @Nullable
    RRTemplate getTemplate() {
        String string = RRUtils.getString(this, RR.template);
        if (string == null) return null;
        return new RRTemplate(string);
    }

    protected @Nullable TermType parseTermType() {
        Resource uri = RRUtils.getURIResource(this, RR.termType);
        if (uri != null) {
            TermType type = TermType.fromResource(uri);
            if (type != null)
                return type;
            throw new InvalidRRException(this, RR.termType, "Unexpected value: " +
                    RR.toString(uri) + "Expected rr:IRI, rr:BlankNode or rr:Literal");
        }
        return null;
    }

    @Override
    public abstract @Nonnull TermType getTermType();

    @Override
    public @Nullable String getLanguage() {
        return RRUtils.getString(this, RR.language);
    }

    @Override
    public @Nullable Resource getDatatype() {
        Resource resource = RRUtils.getURIResource(this, RR.datatype);
        if (resource == null && getLanguage() != null)
            resource = getModel().createResource(RDF.langString);
        return resource;
    }
}
