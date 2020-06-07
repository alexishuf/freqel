package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.collect.ImmutableMap;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class PropertySelectivityCardinalityHeuristic implements CardinalityHeuristic {
    private static final int triplePenalty = 10000000;
    private static final int fallbackDoublePenalty = 25000;
    private static final int fallbackPredicatePenalty = 250;
    private static final int fallbackSubjectPenalty = 5000;
    private static final int fallbackObjectPenalty = 1000;
    private static final Map<Term, Integer> doublePenalty;
    private static final Map<Term, Integer> subjectPenalty;
    private static final Map<Term, Integer> objectPenalty;

    //TODO: compute values from data instead of these educated guesses
    static {
        Map<URI, Integer> map = new HashMap<>();
        map.put(JenaWrappers.fromURIResource(RDF.type),            1000000);
        map.put(JenaWrappers.fromURIResource(DCTerms.title),        500000);
        map.put(JenaWrappers.fromURIResource(RDFS.label),           500000);
        map.put(JenaWrappers.fromURIResource(DCTerms.description),  250000);
        map.put(JenaWrappers.fromURIResource(OWL2.sameAs),          250000);
        map.put(JenaWrappers.fromURIResource(RDFS.seeAlso),         125000);
        map.put(JenaWrappers.fromURIResource(FOAF.name),             60000);
        map.put(JenaWrappers.fromURIResource(FOAF.familyName),       45000);
        map.put(JenaWrappers.fromURIResource(FOAF.givenName),        45000);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox),             45000);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox_sha1sum),     45000);
        doublePenalty = ImmutableMap.copyOf(map);

        map.clear();
        map.put(JenaWrappers.fromURIResource(RDF.type),             50000);
        map.put(JenaWrappers.fromURIResource(RDFS.label),            5000);
        map.put(JenaWrappers.fromURIResource(FOAF.givenName),        5000);
        map.put(JenaWrappers.fromURIResource(FOAF.name),             2500);
        map.put(JenaWrappers.fromURIResource(DCTerms.title),         1000);
        map.put(JenaWrappers.fromURIResource(DCTerms.description),   1000);
        map.put(JenaWrappers.fromURIResource(FOAF.familyName),       1000);
        map.put(JenaWrappers.fromURIResource(RDFS.seeAlso),           300);
        map.put(JenaWrappers.fromURIResource(OWL2.sameAs),            150);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox),              100);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox_sha1sum),      100);
        subjectPenalty = ImmutableMap.copyOf(map);

        map.clear();
        map.put(JenaWrappers.fromURIResource(OWL2.sameAs),           70);
        map.put(JenaWrappers.fromURIResource(DCTerms.title),         50);
        map.put(JenaWrappers.fromURIResource(RDFS.label),            50);
        map.put(JenaWrappers.fromURIResource(DCTerms.description),   50);
        map.put(JenaWrappers.fromURIResource(RDF.type),              20);
        map.put(JenaWrappers.fromURIResource(RDFS.seeAlso),          10);
        map.put(JenaWrappers.fromURIResource(FOAF.name),             10);
        map.put(JenaWrappers.fromURIResource(FOAF.familyName),       10);
        map.put(JenaWrappers.fromURIResource(FOAF.givenName),        10);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox),             10);
        map.put(JenaWrappers.fromURIResource(FOAF.mbox_sha1sum),     10);
        objectPenalty = ImmutableMap.copyOf(map);
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nonnull TPEndpoint ignored) {
        int acc = -1;
        if (query.isJoinConnected()) {
            for (Triple triple : query) acc = min(acc, estimate(triple));
        } else {
            for (Triple triple : query) acc = max(acc, estimate(triple));
        }
        return acc < 0 ? Cardinality.UNSUPPORTED : Cardinality.guess(acc);
    }

    private int min(int left, int right) {
        if ( left < 0) return right;
        if (right < 0) return left;
        return Math.min(left, right);
    }

    private int max(int left, int right) {
        if ( left < 0) return right;
        if (right < 0) return left;
        return Math.max(left, right);
    }

    public int estimate(@Nonnull Triple t) {
        Term s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
        boolean sv = s.isVar(), pv = p.isVar(), ov = o.isVar();
        if (sv && pv && ov) {
            return triplePenalty;
        } else if ((sv && ov) || (sv && pv)) {
            return doublePenalty.getOrDefault(p, fallbackDoublePenalty);
        } else if (pv) { // also pv && ov
            return fallbackPredicatePenalty;
        } else if (sv) {
            return subjectPenalty.getOrDefault(p, fallbackSubjectPenalty);
        } else if (ov) {
            return objectPenalty.getOrDefault(p, fallbackObjectPenalty);
        }
        return -1;
    }
}
