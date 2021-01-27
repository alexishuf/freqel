package br.ufsc.lapesd.freqel.federation.execution;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class InjectedExecutor implements PlanExecutor {
    private final @Nonnull QueryOpExecutor queryNodeExecutor;
    private final @Nonnull DQueryOpExecutor dQueryOpExecutor;
    private final @Nonnull UnionOpExecutor multiQueryNodeExecutor;
    private final @Nonnull JoinOpExecutor joinNodeExecutor;
    private final @Nonnull CartesianOpExecutor cartesianNodeExecutor;
    private final @Nonnull EmptyOpExecutor emptyNodeExecutor;
    private final @Nonnull SPARQLValuesTemplateOpExecutor sparqlValuesTemplateNodeExecutor;

    @Inject
    public InjectedExecutor(@Nonnull QueryOpExecutor queryNodeExecutor,
                            @Nonnull DQueryOpExecutor dQueryOpExecutor,
                            @Nonnull UnionOpExecutor multiQueryNodeExecutor,
                            @Nonnull JoinOpExecutor joinNodeExecutor,
                            @Nonnull CartesianOpExecutor cartesianNodeExecutor,
                            @Nonnull EmptyOpExecutor emptyNodeExecutor,
                            @Nonnull SPARQLValuesTemplateOpExecutor sparqlValuesTemplateNodeExecutor) {
        this.queryNodeExecutor = queryNodeExecutor;
        this.dQueryOpExecutor = dQueryOpExecutor;
        this.multiQueryNodeExecutor = multiQueryNodeExecutor;
        this.joinNodeExecutor = joinNodeExecutor;
        this.cartesianNodeExecutor = cartesianNodeExecutor;
        this.emptyNodeExecutor = emptyNodeExecutor;
        this.sparqlValuesTemplateNodeExecutor = sparqlValuesTemplateNodeExecutor;
    }

    @Override
    public @Nonnull  Results executePlan(@Nonnull Op plan) {
        return executeNode(plan);
    }

    @Override
    public @Nonnull Results executeNode(@Nonnull Op node) {
        assert TreeUtils.isAcyclic(node) : "Node is not a tree";
        assert node.getRequiredInputVars().isEmpty() : "Node needs inputs";
        Class<? extends Op> cls = node.getClass();
        Results results;
        if (EndpointQueryOp.class.isAssignableFrom(cls))
            results = queryNodeExecutor.execute(node);
        else if (DQueryOp.class.isAssignableFrom(cls))
            results = dQueryOpExecutor.execute(node);
        else if (UnionOp.class.isAssignableFrom(cls))
            results = multiQueryNodeExecutor.execute(node);
        else if (JoinOp.class.isAssignableFrom(cls))
            results = joinNodeExecutor.execute(node);
        else if (CartesianOp.class.isAssignableFrom(cls))
            results = cartesianNodeExecutor.execute(node);
        else if (EmptyOp.class.isAssignableFrom(cls))
            results = emptyNodeExecutor.execute(node);
        else if (SPARQLValuesTemplateOp.class.isAssignableFrom(cls))
            results = sparqlValuesTemplateNodeExecutor.execute(node);
        else
            throw new UnsupportedOperationException("No executor for "+cls);
        results.setNodeName(node.getName());
        // optionality of results must always corresponds to optionality in the plan
        assert results.isOptional() == (node.modifiers().optional() != null);
        // it may happen (as an optimization) that a non-distinct node has distinct
        // results because distinct was pushed from the query root to the leaves
        assert node.modifiers().distinct() == null || results.isAsync() || results.isDistinct();
        return results;
    }
}
