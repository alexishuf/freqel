package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl.shortcut;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RRTemplate;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl.TermMapImpl;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class ShortcutMapImpl extends TermMapImpl {
    private @Nonnull final RDFNode constant;

    public ShortcutMapImpl(Node n, EnhGraph m, @Nonnull RDFNode constant) {
        super(n, m);
        this.constant = constant;
    }

    @Override
    public @Nonnull RDFNode getConstant() {
        return constant;
    }

    @Override
    public @Nullable String getColumn() {
        return null;
    }

    @Override
    public @Nullable
    RRTemplate getTemplate() {
        return null;
    }

    @Override
    public @Nonnull TermType getTermType() {
        if (constant.isURIResource())
            return TermType.IRI;
        if (constant.isAnon())
            return TermType.BlankNode;
        if (constant.isLiteral())
            return TermType.Literal;
        throw new UnsupportedOperationException("Unsupported RDFNode type for "+constant);
    }

    @Override
    public @Nullable String getLanguage() {
        if (!constant.isLiteral())
            return null;
        String lang = constant.asLiteral().getLanguage();
        if (lang.isEmpty()) lang = null;
        return lang;
    }

    @Override
    public @Nullable Resource getDatatype() {
        if (constant.isLiteral())
            return getModel().createResource(constant.asLiteral().getDatatypeURI());
        return null;
    }
}
