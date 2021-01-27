package br.ufsc.lapesd.freqel.jena.query;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.util.DQPushChecker;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Source;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.*;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLDisjunctiveProfile;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.modifiers.Limit;
import br.ufsc.lapesd.freqel.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.util.LogUtils;
import com.google.common.base.Stopwatch;
import org.apache.http.client.HttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.system.Txn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.freqel.federation.cardinality.EstimatePolicy.*;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.apache.jena.query.QueryExecutionFactory.sparqlService;

public class ARQEndpoint extends AbstractTPEndpoint implements DQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(ARQEndpoint.class);

    @SuppressWarnings("Immutable")
    private final @Nonnull Function<Query, QueryExecution> executionFactory;
    @SuppressWarnings("Immutable")
    private final @Nullable Transactional transactional;
    @SuppressWarnings("Immutable")
    private final @Nonnull Runnable closer;
    private final @Nullable String name;
    private final boolean local;

    protected ARQEndpoint(@Nullable String name,
                          @Nonnull Function<Query, QueryExecution> executionFactory,
                          @Nullable Transactional transactional,
                          @Nonnull Runnable closer, boolean local) {
        this.executionFactory = executionFactory;
        this.name = name;
        this.transactional = transactional;
        this.closer = closer;
        this.local = local;
    }

    public @Nullable String getName() {
        return name;
    }

    public boolean isLocal() {
        return local;
    }

    public boolean isEmpty() {
        try (QueryExecution execution = createExecution("ASK WHERE {?s ?p ?o.}")) {
            return !execution.execAsk();
        }
    }

    public @Nonnull Source asSource() {
        return new Source(new SelectDescription(this), this);
    }

    /* ~~~ static method factories over some common source types  ~~~ */

    public static ARQEndpoint forModel(@Nonnull Model model) {
        String name = format("%s@%x", model.getClass().getSimpleName(),
                                             System.identityHashCode(model));
        return forModel(model, name);
    }
    public static ARQEndpoint forModel(@Nonnull Model model, @Nonnull String name) {
        return new ARQEndpoint(name, sparql -> create(sparql, model), null,
                               () -> {}, true);
    }
    public static ARQEndpoint forDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds), ds,
                               () -> {}, true);
    }
    public static ARQEndpoint forCloseableDataset(@Nonnull Dataset ds) {
        return new ARQEndpoint(ds.toString(), sparql -> create(sparql, ds),
                               ds, ds::close, true);
    }

    public static ARQEndpoint forService(@Nonnull String uri) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql), null,
                               () -> {}, false);
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client), null,
                               () -> {}, false);
    }
    public static ARQEndpoint forService(@Nonnull String uri, @Nonnull HttpClient client,
                                         @Nonnull HttpContext context) {
        return new ARQEndpoint(uri, sparql -> sparqlService(uri, sparql, client, context),
                               null, () -> {}, false);
    }

    /* ~~~ method overloads and implementations  ~~~ */

    @Override
    public String toString() {
        return name != null ? format("ARQEndpoint(%s)", name) : super.toString();
    }

    @Override
    public boolean hasSPARQLCapabilities() {
        return true;
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case CARTESIAN:
            case LIMIT:
            case SPARQL_FILTER:
            case VALUES:
            case OPTIONAL:
            case ASK:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean canQuerySPARQL() {
        return true;
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        Query query = parseSparql(sparqlQuery);
        Set<String> varNames = query.getProjectVars().stream().map(Var::getVarName)
                                                     .collect(Collectors.toSet());
        return doTransactional(() -> doQuery(query, query.isAskType(), varNames));
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery, boolean isAsk,
                                        @Nonnull Collection<String> varNames) {
        Query query = parseSparql(sparqlQuery);
        Set<String> set = varNames instanceof Set ? (Set<String>)varNames : new HashSet<>(varNames);
        return doTransactional(() -> doQuery(query, isAsk, set));
    }

    @Override
    public @Nonnull DisjunctiveProfile getDisjunctiveProfile() {
        return SPARQLDisjunctiveProfile.DEFAULT;
    }

    @Override
    public @Nonnull Results query(@Nonnull Op query) throws DQEndpointException,
                                                            QueryExecutionException {
        assert query.modifiers().stream().allMatch(m -> hasCapability(m.getCapability()));
        assert new DQPushChecker(getDisjunctiveProfile()).setEndpoint(this).canPush(query);
        SPARQLString ss = SPARQLString.create(query);
        Query jenaQuery = parseSparql(ss.getSparql());
        Results r = doTransactional(() -> doQuery(jenaQuery, ss.isAsk(), ss.getVarNames()));
        r.setOptional(query.modifiers().optional() != null);
        return r;
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) throws QueryExecutionException {
        ModifierUtils.check(this, query.getModifiers());
        SPARQLString ss = SPARQLString.create(query);
        Query jenaQuery = parseSparql(ss.getSparql());
        Results r = doTransactional(() -> doQuery(jenaQuery, ss.isAsk(), ss.getVarNames()));
        r.setOptional(query.getModifiers().optional() != null);
        return r;
    }

    @Override public boolean ignoresAtoms() {
        return true;
    }

    private @Nonnull Query parseSparql(@Nonnull String sparql) {
        try {
            return QueryFactory.create(sparql);
        } catch (QueryException e) { throw new QueryExecutionException(e); }
    }

    private  @Nonnull Results doTransactional(@Nonnull Callable<Results> callable) {
        if (transactional != null) {
            CompletableFuture<Results> future = new CompletableFuture<>();
            Txn.executeRead(transactional, () -> {
                try {
                    future.complete(CollectionResults.greedy(callable.call()));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
            try {
                return future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for transaction");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RuntimeException)
                    throw (RuntimeException)e.getCause();
                throw new RuntimeException("Unexpected ExecutionException", e);
            }
        } else {
            try {
                return callable.call();
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                else
                    throw new RuntimeException("Unexpected Exception", e);
            }
        }
    }

    public @Nonnull Results doQuery(@Nonnull Query query, boolean isAsk,
                                    @Nonnull Set<String> vars) {
        Stopwatch sw = Stopwatch.createStarted();
        if (isAsk) {
            try (QueryExecution exec = executionFactory.apply(query)) {
                boolean ans = exec.execAsk();
                Set<Solution> solutions = ans ? singleton(MapSolution.EMPTY) : emptySet();
                LogUtils.logQuery(logger, query, this, solutions.size(), sw);
                return new CollectionResults(solutions, emptySet());
            } catch (Throwable e) {
                String msg = format("Failed to execute query. Reason: %s. Query: \"\"\"%s\"\"\"",
                                    e.getMessage(), query);
                throw new QueryExecutionException(msg, e);
            }
        } else {
            QueryExecution exec = executionFactory.apply(query);
            try {
                ResultSet rs = exec.execSelect();
                LogUtils.logQuery(logger, query, this, sw);
                return new JenaBindingResults(rs, exec, vars, query.isDistinct());
            } catch (Throwable t) {
                if (exec != null)
                    exec.close();
                String msg = format("Failed to execute query. Reason: %s. Query: \"\"\"%s\"\"\"",
                                    t.getMessage(), query);
                throw new QueryExecutionException(msg, t);
            }
        }
    }

    private @Nonnull QueryExecution createExecution(@Nonnull String string) {
        Query query = QueryFactory.create(string);
        return executionFactory.apply(query);
    }

    @Override public double alternativePenalty(@NotNull CQuery query) {
        return isLocal() ? 0 : 0.5;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return Cardinality.EMPTY;
        if (isLocal() && isEmpty()) return Cardinality.EMPTY;
        if (policy == 0)
            return Cardinality.UNSUPPORTED;
        if (isLocal() && !canLocal(policy))
            return Cardinality.UNSUPPORTED;
        if (!isLocal() && !canRemote(policy))
            return Cardinality.UNSUPPORTED;

        MutableCQuery mQuery = new MutableCQuery(query);
        if ((isLocal() && !canQueryLocal(policy)) || (!isLocal() && !canQueryRemote(policy)))
            mQuery.mutateModifiers().add(Ask.INSTANCE);
        else if ((isLocal() && canAskLocal(policy)) || (!isLocal() && canAskRemote(policy)))
            mQuery.mutateModifiers().add(Limit.of(Math.max(limit(policy), 4)));
        SPARQLString sparql = SPARQLString.create(mQuery);
        if (sparql.isAsk()) {
            try (QueryExecution exec = createExecution(sparql.getSparql())) {
                return exec.execAsk() ? Cardinality.NON_EMPTY : Cardinality.EMPTY;
            }
        } else {
            try (QueryExecution exec = createExecution(sparql.getSparql())) {
                ResultSet results = exec.execSelect();
                int count = 0;
                boolean exhausted = false;
                Stopwatch sw = Stopwatch.createStarted();
                while (!exhausted && sw.elapsed(TimeUnit.MILLISECONDS) < 50) {
                    exhausted = !results.hasNext();
                    if (!exhausted) {
                        results.next();
                        ++count;
                    }
                }
                return count == 0 ? Cardinality.EMPTY
                        : (exhausted ? Cardinality.exact(count) : Cardinality.lowerBound(count));
            }
        }
    }

    @Override
    public void close() {
        closer.run();
    }
}
