package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.PredicateMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;

import javax.annotation.Nonnull;

public class PredicateMapImpl extends TermMapImpl implements PredicateMap {
    public static final @Nonnull
    Implementation factory = new ImplementationByObject(RR.predicateMap) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), PredicateMap.class);
            return new PredicateMapImpl(node, eg);
        }
    };

    public PredicateMapImpl(Node n, EnhGraph m) {
        super(n, m, PredicateMap.class);
    }

    @Override
    public @Nonnull TermType getTermType() {
        TermType type = parseTermType();
        if (type != null && type != TermType.IRI)
            throw new InvalidRRException(this, RR.termType, "Bad value "+type+" expected IRI");
        return TermType.IRI;
    }
}
