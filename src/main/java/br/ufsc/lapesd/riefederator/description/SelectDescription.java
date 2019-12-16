package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.*;
import org.apache.jena.vocabulary.RDF;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A description that performs a SPARQL SELECT against the endpoint to get predicates
 * (and possibly classes).
 */
public class SelectDescription implements Description {
    private static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());

    private final @Nonnull CQEndpoint endpoint;
    private final boolean fetchClasses;
    private @Nullable Set<Term> predicates, classes;

    public SelectDescription(@Nonnull CQEndpoint endpoint) throws MissingCapabilityException {
        this(endpoint, false);
    }

    public SelectDescription(@Nonnull CQEndpoint endpoint,
                             boolean fetchClasses) throws MissingCapabilityException {
        Capability.PROJECTION.requireFrom(endpoint);
        Capability.DISTINCT.requireFrom(endpoint);
        this.endpoint = endpoint;
        this.fetchClasses = fetchClasses;
    }

    private synchronized void ensureHasData() {
        if (predicates != null) return;
        update();
    }

    /**
     * Issues SELECT queries against the SPARQL service at getSparqlAddress()
     */
    public synchronized void update() {
        StdVar s = new StdVar("s"), p = new StdVar("p"), o = new StdVar("o");
        predicates = fill(new Triple(s, p, o), "p");
        if (fetchClasses) classes = fill(new Triple(s, TYPE, o), "o");
    }

    private Set<Term> fill(@Nonnull Triple query, @Nonnull String varName) {
        CQuery cQuery = CQuery.with(query).project(varName).distinct().build();
        try (Results results = endpoint.query(cQuery)) {
            Set<Term> set = new HashSet<>(results.getCardinality().getValue(128));
            while (results.hasNext())
                set.add(requireNonNull(results.next().get(varName)));
            return set;
        }
    }

    public @Nonnull Set<Term> getPredicates() {
        ensureHasData();
        return requireNonNull(predicates);
    }

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        ensureHasData();
        Set<Term> predicates = requireNonNull(this.predicates);
        CQueryMatch.Builder b = CQueryMatch.builder(query);
        for (Triple triple : query) {
            Term p = triple.getPredicate();
            Term o = triple.getObject();
            if (classes != null && p.equals(TYPE) && o.isGround()) {
                if (classes.contains(o))
                    b.addTriple(triple);
            } else if (p.isVar() || predicates.contains(p)) {
                b.addTriple(triple);
            }
        }
        return b.build();
    }
}
