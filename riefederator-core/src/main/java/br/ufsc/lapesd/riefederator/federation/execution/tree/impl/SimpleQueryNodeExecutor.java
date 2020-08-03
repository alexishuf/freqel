package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.SPARQLValuesTemplateNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.*;
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

public class SimpleQueryNodeExecutor extends SimpleNodeExecutor
        implements QueryNodeExecutor, MultiQueryNodeExecutor, CartesianNodeExecutor,
                   SPARQLValuesTemplateNodeExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryNodeExecutor.class);
    private final @Nonnull ResultsExecutor resultsExecutor;

    @Inject
    public SimpleQueryNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                   @Nonnull ResultsExecutor resultsExecutor) {
        super(planExecutorProvider);
        this.resultsExecutor = resultsExecutor;
    }

    @VisibleForTesting
    public SimpleQueryNodeExecutor(@Nonnull PlanExecutor planExecutor,
                            @Nonnull ResultsExecutor resultsExecutor) {
        super(planExecutor);
        this.resultsExecutor = resultsExecutor;
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return QueryNode.class.isAssignableFrom(nodeClass)
                || MultiQueryNode.class.isAssignableFrom(nodeClass)
                || CartesianNode.class.isAssignableFrom(nodeClass)
                || SPARQLValuesTemplateNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) {
        if (node instanceof MultiQueryNode)
            return execute((MultiQueryNode)node);
        else if (node instanceof QueryNode)
            return execute((QueryNode)node);
        else if (node instanceof SPARQLValuesTemplateNode)
            return execute((SPARQLValuesTemplateNode)node);
        throw new IllegalArgumentException("");
    }

    @Override
    public @Nonnull Results execute(@Nonnull SPARQLValuesTemplateNode node) {
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
    public @Nonnull Results execute(@Nonnull QueryNode node) {
        try {
            return doExecute(node);
        } catch (QueryExecutionException e) {
            logger.error("Failed to execute query against {}. Will return an Empty result",
                    node.getEndpoint(), e);
            return CollectionResults.empty(node.getResultVars());
        }
    }

    public @Nonnull Results doExecute(@Nonnull QueryNode node) {
        CQuery query = node.getQuery();
        TPEndpoint ep = node.getEndpoint();
        boolean isSPARQL = ep.hasSPARQLCapabilities();
        boolean canFilter = isSPARQL || ep.hasCapability(Capability.SPARQL_FILTER);
        boolean hasCapabilities = isSPARQL || query.getModifiers().stream()
                .allMatch(m -> ep.hasCapability(m.getCapability()))
                && (node.getFilters().isEmpty() || canFilter) ;

        MutableCQuery mQuery = new MutableCQuery(query);
        Set<SPARQLFilter> removed = mQuery.sanitizeFiltersStrict();
        if (!removed.isEmpty()) {
            logger.warn("Query had {} filters with input variables. Such variables should've " +
                        "been bound before execution against {}. Offending filters: {}. " +
                        "Offending query: {}", removed.size(), ep, removed, query);
            assert false : "Attempted to execute query with input vars in filters";
        }

        if (hasCapabilities) {
            mQuery.addModifiers(node.getFilters());
            return ep.query(mQuery);
        } else { // endpoint cannot handle some modifiers, not even locally
            mQuery.removeModifierIf(m -> !ep.hasCapability(m.getCapability()));
            if (canFilter) mQuery.addModifiers(node.getFilters());
            Results results = ep.query(mQuery);

            if (query.attr().isDistinct() && !mQuery.attr().isDistinct())
                results = HashDistinctResults.applyIf(results, query);
            if (!canFilter) {
                Set<SPARQLFilter> set = TreeUtils.union(query.attr().filters(), node.getFilters());
                results = SPARQLFilterResults.applyIf(results, set);
            }
            if (query.attr().limit() > 0 && mQuery.attr().limit() <= 0)
                results = LimitResults.applyIf(results, query);
            results = ProjectingResults.applyIf(results, query);
            if (query.attr().isAsk() && !mQuery.attr().isAsk())
                results  = AskResults.applyIf(results, query);

            return results;
        }
    }

    @CheckReturnValue
    public @Nonnull Results executeAsMultiQuery(@Nonnull PlanNode node) {
        if (node.getChildren().isEmpty())
            return new CollectionResults(Collections.emptyList(), node.getResultVars());
        ArrayList<Results> resultList = new ArrayList<>(node.getChildren().size());
        PlanExecutor executor = getPlanExecutor();
        for (PlanNode child : node.getChildren()) {
            node.getFilters().forEach(child::addFilter);
            resultList.add(executor.executeNode(child));
        }
        Results r = resultsExecutor.async(resultList, node.getResultVars());
        return node.isProjecting() ? new ProjectingResults(r, node.getResultVars()) : r;
    }

    @Override
    public @Nonnull Results execute(@Nonnull MultiQueryNode node) {
        return executeAsMultiQuery(node);
    }

    /**
     * Execute a {@link CartesianNode} without doing a cartesian product. This is
     * nonconconformant and highly confusing, but may be useful if the user wants it
     */
    @Override
    public @Nonnull Results execute(@Nonnull CartesianNode node) {
        return executeAsMultiQuery(node);
    }
}
