package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.ObjectMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

public class ObjectMapImpl extends TermMapImpl implements ObjectMap {
    private static final Pattern IRI_RX = Pattern.compile("^([a-zA-z0-9]+://|mailto:|urn:)");

    public static final @Nonnull
    Implementation factory = new Implementation() {
        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            if (ReferencingObjectMapImpl.factory.canWrap(node, eg))
                return true;
            return eg.asGraph().contains(null, RR.objectMap.asNode(), node);
        }

        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (ReferencingObjectMapImpl.factory.canWrap(node, eg))
                return ReferencingObjectMapImpl.factory.wrap(node, eg);
            if (!eg.asGraph().contains(null, RR.objectMap.asNode(), node))
                throw new InvalidRRException(node, eg.asGraph(), ObjectMap.class);
            return new ObjectMapImpl(node, eg);
        }
    };

    public ObjectMapImpl(Node n, EnhGraph m) {
        super(n, m, ObjectMap.class);
    }

    public ObjectMapImpl(Node n, EnhGraph m, Class<? extends TermMap> implClass) {
        super(n, m, implClass);
    }

    @Override
    public @Nonnull TermType getTermType() {
        TermType type = parseTermType();
        if (type != null)
            return type;
        // Rules below from § 7.4 at https://www.w3.org/TR/r2rml/#termtype
        if (getLanguage() != null)
            return TermType.Literal;
        if (getDatatype() != null)
            return TermType.Literal;
        if (getColumn() != null)
            return TermType.Literal;
        return TermType.IRI;
    }
}
