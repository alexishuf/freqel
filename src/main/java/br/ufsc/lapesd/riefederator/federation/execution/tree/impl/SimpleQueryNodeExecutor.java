package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.*;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

public class SimpleQueryNodeExecutor extends SimpleNodeExecutor
        implements QueryNodeExecutor, MultiQueryNodeExecutor, CartesianNodeExecutor {
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
                || CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) {
        if (node instanceof MultiQueryNode)
            return execute((MultiQueryNode)node);
        else if (node instanceof QueryNode)
            return execute((QueryNode)node);
        throw new IllegalArgumentException("");
    }

    @Override
    public @Nonnull Results execute(@Nonnull QueryNode node) {
        CQuery query = node.getQuery();
        TPEndpoint ep = node.getEndpoint();
        boolean canFilter = ep.hasCapability(Capability.SPARQL_FILTER);
        boolean hasCapabilities = query.getModifiers().stream()
                .allMatch(m -> ep.hasCapability(m.getCapability()))
                && (node.getFilters().isEmpty() || canFilter) ;

        if (hasCapabilities) {
            if (query.getModifiers().containsAll(node.getFilters()))
                return ep.query(query);

            CQuery.WithBuilder b = CQuery.with(query).copyModifiers(query).copyAnnotations(query);
            for (SPARQLFilter filter : node.getFilters())
                b.modifier(filter);
            CQuery augmented = b.build();
            assert augmented.getModifiers().containsAll(query.getModifiers());
            return ep.query(augmented);
        } else {
            // endpoint cannot handle some modifiers, not even locally
            Projection projection = null;
            Distinct distinct = null;
            Ask ask = null;
            CQuery.WithBuilder b = CQuery.with(query.getList()).copyAnnotations(query);
            for (Modifier m : query.getModifiers()) {
                if (ep.hasCapability(m.getCapability())) b.modifier(m);
                else if (m instanceof Ask)               ask = (Ask)m;
                else if (m instanceof Projection)        projection = (Projection)m;
                else if (m instanceof Distinct)          distinct = (Distinct)m;
            }
            if (canFilter) {
                for (SPARQLFilter filter : node.getFilters())
                    b.modifier(filter);
            }
            Results results = ep.query(b.build());

            if (!canFilter) {
                List<SPARQLFilter> filters = new ArrayList<>();
                query.getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                                    .map(m -> (SPARQLFilter)m).forEach(filters::add);
                filters.addAll(node.getFilters());
                results = new SPARQLFilterResults(results, filters);
            }
            if (projection != null)
                results = new ProjectingResults(results, projection.getVarNames());
            if (ask != null) {
                Set<String> vs = results.getVarNames();
                return results.hasNext() ? new CollectionResults(singleton(MapSolution.EMPTY), vs)
                                         : CollectionResults.empty(vs);
            }
            if (distinct != null)
                results = new HashDistinctResults(results);
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
        Results r = resultsExecutor.async(resultList);
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
