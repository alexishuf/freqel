package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.SPARQLValuesTemplateOpExecutor;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.QueryExecutionException;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
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
import java.util.Collections;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;

public class SimpleQueryOpExecutor extends SimpleOpExecutor
        implements QueryOpExecutor, MultiQueryOpExecutor, CartesianOpExecutor,
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
        return QueryOp.class.isAssignableFrom(nodeClass)
                || UnionOp.class.isAssignableFrom(nodeClass)
                || CartesianOp.class.isAssignableFrom(nodeClass)
                || SPARQLValuesTemplateOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) {
        if (node instanceof UnionOp)
            return execute((UnionOp)node);
        else if (node instanceof QueryOp)
            return execute((QueryOp)node);
        else if (node instanceof SPARQLValuesTemplateOp)
            return execute((SPARQLValuesTemplateOp)node);
        throw new IllegalArgumentException("");
    }

    @Override
    public @Nonnull Results execute(@Nonnull SPARQLValuesTemplateOp node) {
        CQEndpoint endpoint = (CQEndpoint) node.getEndpoint();
        assert endpoint.canQuerySPARQL();
        try {
            return endpoint.querySPARQL(node.createSPARQL(), node.isAsk(), node.getResultVars());
        } catch (QueryExecutionException e) {
            logger.error("Failed to execute SPARQL query against {}. Will return an Empty result",
                         node.getEndpoint(), e);
            return CollectionResults.empty(node.getResultVars());
        }
    }

    @Override
    public @Nonnull Results execute(@Nonnull QueryOp node) {
        try {
            return doExecute(node);
        } catch (QueryExecutionException e) {
            logger.error("Failed to execute query against {}. Will return an Empty result",
                    node.getEndpoint(), e);
            return CollectionResults.empty(node.getResultVars());
        }
    }

    public @Nonnull Results doExecute(@Nonnull QueryOp node) {
        CQuery q = node.getQuery();
        TPEndpoint ep = node.getEndpoint();
        Set<SPARQLFilter> filters = node.modifiers().filters();
        boolean isSPARQL = ep.hasSPARQLCapabilities();
        boolean canFilter = isSPARQL || ep.hasCapability(Capability.SPARQL_FILTER);
        boolean hasCapabilities = isSPARQL || q.getModifiers().stream()
                .allMatch(m -> ep.hasCapability(m.getCapability()))
                && (filters.isEmpty() || canFilter) ;

        MutableCQuery mq = new MutableCQuery(q);
        Set<SPARQLFilter> removed = mq.sanitizeFiltersStrict();
        if (!removed.isEmpty()) {
            logger.warn("Query had {} filters with input variables. Such variables should've " +
                        "been bound before execution against {}. Offending filters: {}. " +
                        "Offending query: {}", removed.size(), ep, removed, q);
            assert false : "Attempted to execute query with input vars in filters";
        }

        if (hasCapabilities) {
            return ep.query(mq);
        } else { // endpoint cannot handle some modifiers, not even locally
            mq.mutateModifiers().removeIf(m -> !ep.hasCapability(m.getCapability()));
            Results r = ep.query(mq);

            if (!ep.hasCapability(Capability.DISTINCT))
                r = HashDistinctResults.applyIf(r, q);
            if (!canFilter)
                r = SPARQLFilterResults.applyIf(r, union(q.getModifiers().filters(), filters));
            if (!ep.hasCapability(Capability.LIMIT))
                r = LimitResults.applyIf(r, q);
            r = ProjectingResults.applyIf(r, q);
            if (!ep.hasCapability(Capability.ASK))
                r  = AskResults.applyIf(r, q);

            return r;
        }
    }

    @CheckReturnValue
    public @Nonnull Results executeAsMultiQuery(@Nonnull Op node) {
        if (node.getChildren().isEmpty())
            return new CollectionResults(Collections.emptyList(), node.getResultVars());
        ArrayList<Results> resultList = new ArrayList<>(node.getChildren().size());
        PlanExecutor executor = getPlanExecutor();
        for (Op child : node.getChildren()) {
            node.modifiers().filters().forEach(f -> child.modifiers().add(f));
            resultList.add(executor.executeNode(child));
        }
        Results r = resultsExecutor.async(resultList, node.getResultVars());
        return ProjectingResults.applyIf(r, node);
    }

    @Override
    public @Nonnull Results execute(@Nonnull UnionOp node) {
        return executeAsMultiQuery(node);
    }

    /**
     * Execute a {@link CartesianOp} without doing a cartesian product. This is
     * nonconconformant and highly confusing, but may be useful if the user wants it
     */
    @Override
    public @Nonnull Results execute(@Nonnull CartesianOp node) {
        return executeAsMultiQuery(node);
    }
}
