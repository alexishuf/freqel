package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.*;
import br.ufsc.lapesd.freqel.algebra.util.DQPushChecker;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.ResultsUtils;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.reason.tbox.EndpointReasoner;
import br.ufsc.lapesd.freqel.reason.tbox.NoEndpointReasoner;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.*;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.exposeFilterVars;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.iteratePreOrder;

public class SimpleQueryOpExecutor extends SimpleOpExecutor
        implements QueryOpExecutor, UnionOpExecutor, DQueryOpExecutor, CartesianOpExecutor,
        SPARQLValuesTemplateOpExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryOpExecutor.class);
    private final @Nonnull ResultsExecutor resultsExecutor;
    private final @Nonnull EndpointReasoner endpointReasoner;

    @Inject
    public SimpleQueryOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                 @Nonnull ResultsExecutor resultsExecutor,
                                 @Nonnull EndpointReasoner endpointReasoner) {
        super(planExecutorProvider);
        this.resultsExecutor = resultsExecutor;
        this.endpointReasoner = endpointReasoner;
    }

    @VisibleForTesting
    public SimpleQueryOpExecutor(@Nonnull PlanExecutor planExecutor,
                                 @Nonnull ResultsExecutor resultsExecutor) {
        super(planExecutor);
        this.resultsExecutor = resultsExecutor;
        this.endpointReasoner = NoEndpointReasoner.INSTANCE;
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return EndpointQueryOp.class.isAssignableFrom(nodeClass)
                || DQueryOpExecutor.class.isAssignableFrom(nodeClass)
                || UnionOp.class.isAssignableFrom(nodeClass)
                || CartesianOp.class.isAssignableFrom(nodeClass)
                || SPARQLValuesTemplateOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) {
        Results results;
        if (node instanceof UnionOp)
            results = execute((UnionOp)node);
        else if (node instanceof EndpointQueryOp)
            results = execute((EndpointQueryOp)node);
        else if (node instanceof DQueryOp)
            results = execute((DQueryOp) node);
        else if (node instanceof SPARQLValuesTemplateOp)
            results = execute((SPARQLValuesTemplateOp)node);
        else
            throw new IllegalArgumentException("Unexpected node class "+node.getClass());

        assert node.modifiers().optional() == null || results.isOptional();
        assert results.getVarNames().containsAll(node.getResultVars());
        assert node.getPublicVars().containsAll(results.getVarNames());
        return results;
    }

    @Override
    public @Nonnull Results execute(@Nonnull SPARQLValuesTemplateOp node) {
        CQEndpoint endpoint = (CQEndpoint) node.getEndpoint();
        assert endpoint.canQuerySPARQL();
        try {
            Results results = endpoint.querySPARQL(node.createSPARQL(), node.isAsk(),
                                                   node.getResultVars());
            results.setOptional(node.modifiers().optional() != null);
            return results;
        } catch (QueryExecutionException e) {
            logger.error("Failed to execute SPARQL query against {}. Will return an Empty result",
                         node.getEndpoint(), e);
            return CollectionResults.empty(node.getResultVars());
        }
    }

    @Override public @Nonnull Results execute(@Nonnull DQueryOp node) {
        try {
            return doExecute(node);
        } catch (QueryExecutionException e) {
            logger.error("Failed execute query against endpoint {}. Cause: {}.\n  Query:\n    {}",
                         node.getEndpoint(), e.getMessage(),
                         node.getQuery().prettyPrint(new StringBuilder(), "    "));
            return CollectionResults.empty(node.getResultVars());
        }
    }

    private static class PreprocessedQuery {
        EndpointOp op;
        boolean hasCopy = false;
        ModifiersSet modifiers;
        @Nullable ModifiersSet pending;

        public PreprocessedQuery(@Nonnull EndpointOp op) {
            setOp(op);
            pending = getPendingModifiers(op.getEndpoint());
            handleRequiredInputs();
        }

        private void handleRequiredInputs() {
            if (op.hasRequiredInputs()) {
                assert false : "Query has required input vars";
                // in production: try to recover by discarding FILTER()s
                copy();
                Set<SPARQLFilter> removed;
                if (op instanceof EndpointQueryOp) {
                    removed = eqOp().getQuery().sanitizeFiltersStrict();
                } else {
                    removed = new HashSet<>();
                    for (Iterator<Op> it = iteratePreOrder(dqOp().getQuery()); it.hasNext(); ) {
                        Op op = it.next();
                        if (op instanceof QueryOp)
                            removed.addAll(((QueryOp) op).getQuery().sanitizeFiltersStrict());
                    }
                }
                logger.warn("Query against {} had unbound required input variables.\n  Offending " +
                            "filters: {}\n  Pending inputs after removed filters: {}\n  Offending " +
                            "query:\n    {}", op.getEndpoint(), removed, op.getRequiredInputVars(),
                            SPARQLString.create(op).getSparql().replace("\n", "\n    "));
            }
        }

        private void setOp(@Nonnull EndpointOp op) {
            this.op = op;
            this.modifiers = op.modifiers();
            assert op instanceof EndpointQueryOp || op instanceof DQueryOp;
        }

        @Nonnull EndpointQueryOp eqOp() { return (EndpointQueryOp) op;}
        @Nonnull DQueryOp dqOp() { return (DQueryOp) op;}

        private @Nullable ModifiersSet getPendingModifiers(@Nonnull TPEndpoint ep) {
            for (Modifier m : modifiers) {
                Capability capability = m.getCapability();
                if (!ep.hasCapability(capability))
                    (pending == null ? pending = new ModifiersSet() : pending).add(m);
            }
            if (!ep.hasCapability(Capability.SPARQL_FILTER) && !modifiers.filters().isEmpty()) {
                assert pending != null;
                copy();
                pending.add(exposeFilterVars(modifiers, modifiers.filters()));
                if (pending.add(modifiers.limit())) modifiers.remove(modifiers.limit());
                if (pending.add(modifiers.ask()  )) modifiers.remove(modifiers.ask()  );
            }
            if (pending != null) {
                assert !pending.isEmpty();
                copy();
                modifiers.removeAll(pending);
            }
            return pending;
        }

        private void copy() {
            if (!hasCopy) setOp((EndpointOp) op.flatCopy());
            hasCopy = true;
        }
    }

    protected @Nonnull Results doExecute(@Nonnull DQueryOp node) {
        DQEndpoint ep = node.getEndpoint();
        PreprocessedQuery data = new PreprocessedQuery(node);
        if (data.pending != null && data.pending.reasoning() != null) {
            if (!endpointReasoner.acceptDisjunctive())
                return getPlanExecutor().executeNode(node.getQuery());
            return endpointReasoner.apply(data.dqOp(), this::execute);
        } else {
            Op query = data.dqOp().getQuery();
            assert new DQPushChecker(ep.getDisjunctiveProfile()).setEndpoint(ep).canPush(query)
                    : "Why did a non-executable plan got to this point!?";
            return ResultsUtils.applyModifiers(ep.query(query), data.pending);
        }
    }

    @Override
    public @Nonnull Results execute(@Nonnull EndpointQueryOp node) {
        try {
            return doExecute(node);
        } catch (QueryExecutionException e) {
            logger.error("Failed to execute query against {}. Will return an Empty result",
                    node.getEndpoint(), e);
            return CollectionResults.empty(node.getResultVars());
        }
    }

    protected @Nonnull Results doExecute(@Nonnull EndpointQueryOp node) {
        TPEndpoint ep = node.getEndpoint();
        PreprocessedQuery data = new PreprocessedQuery(node);
        if (data.pending != null && data.pending.reasoning() != null)
            return endpointReasoner.apply(data.eqOp(), this::execute);
        CQuery q = data.eqOp().getQuery();
        return ResultsUtils.applyModifiers(ep.query(q), data.pending);
    }

    @CheckReturnValue
    public @Nonnull Results executeAsUnion(@Nonnull Op node) {
        int size = node.getChildren().size();
        if (size == 0)
            return new CollectionResults(Collections.emptyList(), node.getResultVars());
        ArrayList<Results> resultList = new ArrayList<>(size);
        PlanExecutor executor = getPlanExecutor();
        Set<SPARQLFilter> unionFilters = node.modifiers().filters();
        for (Op child : node.getChildren())
            resultList.add(executor.executeNode(pushingFilters(child, unionFilters)));
        Results results = size > 1 ? resultsExecutor.async(resultList, node.getResultVars())
                                   : resultList.get(0);
        return ResultsUtils.applyNonFilterModifiers(results, node.modifiers());
    }

    private @Nonnull Op pushingFilters(@Nonnull Op original,
                                       @Nonnull Collection<SPARQLFilter> filters) {
        if (original.modifiers().filters().containsAll(filters))
            return original;
        Op copy = original.flatCopy();
        copy.modifiers().addAll(filters);
        return copy;
    }

    @Override
    public @Nonnull Results execute(@Nonnull UnionOp node) {
        return executeAsUnion(node);
    }

    /**
     * Execute a {@link CartesianOp} without doing a cartesian product. This is
     * nonconconformant and highly confusing, but may be useful if the user wants it
     */
    @Override
    public @Nonnull Results execute(@Nonnull CartesianOp node) {
        return executeAsUnion(node);
    }
}
