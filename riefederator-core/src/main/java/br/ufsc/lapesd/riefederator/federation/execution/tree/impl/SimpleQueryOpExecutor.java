package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.riefederator.algebra.util.DQPushChecker;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.*;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.*;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;

public class SimpleQueryOpExecutor extends SimpleOpExecutor
        implements QueryOpExecutor, UnionOpExecutor, DQueryOpExecutor, CartesianOpExecutor,
        SPARQLValuesTemplateOpExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryOpExecutor.class);
    private final @Nonnull ResultsExecutor resultsExecutor;

    @Inject
    public SimpleQueryOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                 @Nonnull ResultsExecutor resultsExecutor) {
        super(planExecutorProvider);
        this.resultsExecutor = resultsExecutor;
    }

    @VisibleForTesting
    public SimpleQueryOpExecutor(@Nonnull PlanExecutor planExecutor,
                                 @Nonnull ResultsExecutor resultsExecutor) {
        super(planExecutor);
        this.resultsExecutor = resultsExecutor;
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
        if (node instanceof UnionOp)
            return execute((UnionOp)node);
        else if (node instanceof EndpointQueryOp)
            return execute((EndpointQueryOp)node);
        else if (node instanceof SPARQLValuesTemplateOp)
            return execute((SPARQLValuesTemplateOp)node);
        throw new IllegalArgumentException("");
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

    protected @Nonnull Results doExecute(@Nonnull DQueryOp node) {
        DQEndpoint ep = node.getEndpoint();
        Op query = node.getQuery();
        assert new DQPushChecker(ep.getDisjunctiveProfile()).setEndpoint(ep).canPush(query)
               : "Why did a non-executable plan got to this point!?";
        // if save all unsupported modifiers in pending
        ModifiersSet pending = null;
        for (Modifier m : query.modifiers()) {
            Capability capability = m.getCapability();
            if (!ep.hasCapability(capability))
                (pending == null ? pending = new ModifiersSet() : pending).add(m);
        }
        if (pending != null) { // replace query to a copy with supported modifiers
            Op copy = TreeUtils.deepCopy(query);
            for (Modifier m : query.modifiers()) {
                if (!pending.contains(m))
                    copy.modifiers().add(m);
            }
            query = copy;
        }

        Results r = ep.query(query);
        if (pending != null) { //apply modifiers locally
            if (pending.optional() != null)
                r.setOptional(true);
            r = HashDistinctResults.applyIf(r, pending);
            r = SPARQLFilterResults.applyIf(r, pending.filters());
            r = LimitResults.applyIf(r, pending);
            r = ProjectingResults.applyIf(r, pending);
            r = AskResults.applyIf(r, pending);
        }
        return r;
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
        CQuery q = node.getQuery();
        TPEndpoint ep = node.getEndpoint();
        Set<SPARQLFilter> filters = node.modifiers().filters();
        boolean isSPARQL = ep.hasSPARQLCapabilities();
        boolean canFilter = isSPARQL || ep.hasCapability(Capability.SPARQL_FILTER);
        boolean hasCapabilities = isSPARQL || q.getModifiers().stream()
                .allMatch(m -> m.equals(Optional.INSTANCE) || ep.hasCapability(m.getCapability()))
                && (filters.isEmpty() || canFilter) ;

        MutableCQuery mq = new MutableCQuery(q);
        Set<SPARQLFilter> removed = mq.sanitizeFiltersStrict();
        if (!removed.isEmpty()) {
            logger.warn("Query had {} filters with input variables. Such variables should've " +
                        "been bound before execution against {}. Offending filters: {}. " +
                        "Offending query: {}", removed.size(), ep, removed, q);
            assert false : "Attempted to execute query with input vars in filters";
        }
        boolean isOptional = mq.mutateModifiers().remove(Optional.INSTANCE);
        assert mq.getModifiers().optional() == null;
        assert !isOptional || node.modifiers().optional() != null; //do not affect input!

        if (hasCapabilities) {
            Results r = ep.query(mq);
            if (isOptional)
                r.setOptional(true);
            return r;
        } else { // endpoint cannot handle some modifiers, not even locally
            mq.mutateModifiers().removeIf(m -> !ep.hasCapability(m.getCapability()));
            Results r = ep.query(mq);
            if (isOptional)
                r.setOptional(true);
            if (!ep.hasCapability(Capability.DISTINCT))
                r = HashDistinctResults.applyIf(r, q);
            if (!canFilter)
                r = SPARQLFilterResults.applyIf(r, union(q.getModifiers().filters(), filters));
            if (!ep.hasCapability(Capability.LIMIT))
                r = LimitResults.applyIf(r, q);
            r = ProjectingResults.applyIf(r, q);
            if (!ep.hasCapability(Capability.ASK))
                r  = AskResults.applyIf(r, q);

            assert !isOptional || r.isOptional();
            return r;
        }
    }

    @CheckReturnValue
    public @Nonnull Results executeAsUnion(@Nonnull Op node) {
        if (node.getChildren().isEmpty())
            return new CollectionResults(Collections.emptyList(), node.getResultVars());
        ArrayList<Results> resultList = new ArrayList<>(node.getChildren().size());
        PlanExecutor executor = getPlanExecutor();
        Set<SPARQLFilter> unionFilters = node.modifiers().filters();
        for (Op child : node.getChildren())
            resultList.add(executor.executeNode(pushingFilters(child, unionFilters)));
        Results results = resultsExecutor.async(resultList, node.getResultVars());
        results.setOptional(node.modifiers().optional() != null);
        return results;
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
