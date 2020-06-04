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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;

/**
 * A description that performs a SPARQL SELECT against the endpoint to get predicates
 * (and possibly classes).
 */
public class SelectDescription implements Description {
    private static final Logger logger = LoggerFactory.getLogger(SelectDescription.class);
    protected static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());

    private final @Nonnull CQEndpoint endpoint;
    private final boolean fetchClasses;
    protected @Nullable Set<Term> predicates, classes;
    private @Nullable Future<?> updateTask = null;

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

    protected boolean ensureHasData() {
        init();
        return waitForInit(60000);
    }

    /**
     * Issues SELECT queries against the SPARQL service at getSparqlAddress()
     */
    @Override
    public boolean updateSync(int timeoutMilliseconds) {
        Future<?> localTask;
        synchronized (this) {
            if (updateTask == null) {
                if (predicates == null)
                    updateTask = ForkJoinPool.commonPool().submit(this::doUpdate);
                else
                    return true;
            }
            localTask = this.updateTask;
        }
        try {
            localTask.get(timeoutMilliseconds, TimeUnit.MILLISECONDS);
            synchronized (this) {
                if (updateTask == localTask)
                    updateTask = null;
            }
            return true;
        } catch (InterruptedException e) {
            logger.warn("updateSync interrupted");
        } catch (ExecutionException e) {
            logger.error("Failed to update description. Will not match anything", e);
            synchronized (this) {
                if (localTask == updateTask) {
                    updateTask = null;
                    if (predicates == null) predicates = Collections.emptySet();
                    if (fetchClasses && classes == null) classes = Collections.emptySet();
                }
            }
        } catch (TimeoutException e) {
            logger.warn("updateSync timed out after {} ms", timeoutMilliseconds);
        }
        return false;
    }

    private void doUpdate() {
        StdVar s = new StdVar("s"), p = new StdVar("p"), o = new StdVar("o");
        Set<Term> predicates = fill(new Triple(s, p, o), "p");
        synchronized (this) {
            this.predicates = predicates;
        }
        if (fetchClasses) {
            Set<Term> classes = fill(new Triple(s, TYPE, o), "o");
            synchronized (this) {
                this.classes = classes;
            }
        }
    }

    @Override
    public synchronized void update() {
        if (updateTask == null)
            updateTask = ForkJoinPool.commonPool().submit(this::doUpdate);
    }

    @Override
    public synchronized void init() {
        if (predicates == null) update();
    }

    @Override
    public boolean waitForInit(int timeoutMilliseconds) {
        synchronized (this) {
            boolean isInitialized = predicates != null && (!fetchClasses || classes != null);
            if      (isInitialized)      return true; // is ready
            else if (updateTask == null) return false; // init() not called
            // else: must wait (outside of the monitor)
        }
        updateSync(timeoutMilliseconds);
        return predicates != null && (!fetchClasses || classes != null);
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

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        CQueryMatch.Builder b = CQueryMatch.builder(query);
        if (!ensureHasData())
            return b.build(); //return empty result if timed out or error
        Set<Term> predicates = requireNonNull(this.predicates);
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
