package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class InjectedExecutor implements PlanExecutor {
    private final @Nonnull QueryOpExecutor queryNodeExecutor;
    private final @Nonnull UnionOpExecutor multiQueryNodeExecutor;
    private final @Nonnull JoinOpExecutor joinNodeExecutor;
    private final @Nonnull CartesianOpExecutor cartesianNodeExecutor;
    private final @Nonnull EmptyOpExecutor emptyNodeExecutor;
    private final @Nonnull SPARQLValuesTemplateOpExecutor sparqlValuesTemplateNodeExecutor;

    @Inject
    public InjectedExecutor(@Nonnull QueryOpExecutor queryNodeExecutor,
                            @Nonnull UnionOpExecutor multiQueryNodeExecutor,
                            @Nonnull JoinOpExecutor joinNodeExecutor,
                            @Nonnull CartesianOpExecutor cartesianNodeExecutor,
                            @Nonnull EmptyOpExecutor emptyNodeExecutor,
                            @Nonnull SPARQLValuesTemplateOpExecutor sparqlValuesTemplateNodeExecutor) {
        this.queryNodeExecutor = queryNodeExecutor;
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
        return results;
    }
}
