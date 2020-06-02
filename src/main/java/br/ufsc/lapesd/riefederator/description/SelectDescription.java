package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.MissingCapabilityException;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.util.LogUtils;
import com.google.common.base.Stopwatch;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(SelectDescription.class);
    protected static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());

    private final @Nonnull
    CQEndpoint endpoint;
    private final boolean fetchClasses;
    protected @Nullable Set<Term> predicates, classes;

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

    protected synchronized void ensureHasData() {
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
        Stopwatch sw = Stopwatch.createStarted();
        CQuery cQuery = CQuery.with(query).project(varName).distinct().build();
        Set<Term> set = null;
        try (Results results = endpoint.query(cQuery)) {
            set = new HashSet<>(results.getCardinality().getValue(128));
            while (results.hasNext())
                set.add(requireNonNull(results.next().get(varName)));
            return set;
        } finally {
            assert set != null;
            LogUtils.logQuery(logger, cQuery, endpoint, set.size(), sw);
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
