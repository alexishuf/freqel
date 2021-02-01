package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.freqel.algebra.util.DQPushChecker;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.*;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.ResultsUtils;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.exposeFilterVars;

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
        else if (node instanceof DQueryOp)
            return execute((DQueryOp) node);
        else if (node instanceof SPARQLValuesTemplateOp)
            return execute((SPARQLValuesTemplateOp)node);
        throw new IllegalArgumentException("Unexpected node class "+node.getClass());
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

    protected @Nullable ModifiersSet getPendingModifiers(@Nonnull ModifiersSet in,
                                                         @Nonnull TPEndpoint ep) {
        ModifiersSet pending = null;
        for (Modifier m : in) {
            Capability capability = m.getCapability();
            if (!ep.hasCapability(capability))
                (pending == null ? pending = new ModifiersSet() : pending).add(m);
        }
        if (!ep.hasCapability(Capability.SPARQL_FILTER) && pending != null) {
            pending.add(exposeFilterVars(in, in.filters()));
            if (pending.add(in.limit())) in.remove(in.limit());
            if (pending.add(in.ask()  )) in.remove(in.ask()  );
        }
        return pending;
    }

    protected @Nonnull Results doExecute(@Nonnull DQueryOp node) {
        DQEndpoint ep = node.getEndpoint();
        Op query = node.getQuery();
        assert new DQPushChecker(ep.getDisjunctiveProfile()).setEndpoint(ep).canPush(query)
               : "Why did a non-executable plan got to this point!?";
        // if save all unsupported modifiers in pending
        ModifiersSet pending = getPendingModifiers(query.modifiers(), ep);
        if (pending != null) { // replace query to a copy with supported modifiers
            Op copy = TreeUtils.deepCopy(query);
            for (Modifier m : query.modifiers()) {
                if (!pending.contains(m))
                    copy.modifiers().add(m);
            }
            query = copy;
        }
        return ResultsUtils.applyModifiers(ep.query(query), pending);
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
        ModifiersSet pending = getPendingModifiers(q.getModifiers(), ep);
        if (!q.attr().reqInputVarNames().isEmpty()) {
            assert false : "Query has required input vars";
            // in production: try to recover by discarding FILTER()s
            MutableCQuery mq = new MutableCQuery(q);
            Set<SPARQLFilter> removed = mq.sanitizeFiltersStrict();
            logger.warn("Query against {} had unbound required input variables.\n  Offending " +
                        "filters: {}\n  Pending inputs after removed filters: {}\n  Offending " +
                        "query:\n    {}", ep, removed, mq.attr().reqInputVarNames(),
                        q.toString().replace("\n", "\n    "));
            q = mq;
        }
        Results r;
        if (pending == null) {
            r = ep.query(q);
        } else {
            MutableCQuery mq = new MutableCQuery(q);
            mq.mutateModifiers().removeIf(pending::contains);
            assert q.getModifiers().containsAll(pending); // q not affected!
            r = ResultsUtils.applyModifiers(ep.query(mq), pending);
        }
        assert q.getModifiers().optional() == null || r.isOptional();
        assert r.getVarNames().equals(q.attr().publicVarNames());
        return r;
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
