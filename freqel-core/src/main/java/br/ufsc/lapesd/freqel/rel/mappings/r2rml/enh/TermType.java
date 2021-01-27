package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public enum TermType {
    IRI,
    BlankNode,
    Literal;

    public static @Nullable TermType fromResource(@Nonnull Resource resource) {
        if (resource.getURI().equals(RR.IRI.getURI()))
            return IRI;
        else if (resource.getURI().equals(RR.BlankNode.getURI()))
            return BlankNode;
        else if (resource.getURI().equals(RR.Literal.getURI()))
            return Literal;
        return null;
    }

    public boolean accepts(@Nonnull RDFNode node) {
        switch (this) {
            case IRI:
                return node.isURIResource();
            case BlankNode:
                return node.isAnon();
            case Literal:
                return node.isLiteral();
        }
        throw new UnsupportedOperationException();
    }

    public @Nonnull Resource getResource() {
        switch (this) {
            case IRI:
                return RR.IRI;
            case BlankNode:
                return RR.BlankNode;
            case Literal:
                return RR.Literal;
        }
        throw new UnsupportedOperationException();
    }

}
