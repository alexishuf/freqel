package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.ComponentNode;
import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class NaiveOuterPlanner implements OuterPlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public NaiveOuterPlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    public NaiveOuterPlanner() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override
    public String toString() {
        return "NaiveOuterPlanner";
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery query) {
        try (TimeSampler ignored = Metrics.OUT_PLAN_MS.createThreadSampler(performance)) {
            if (query.isEmpty())
                return new EmptyNode(query);
            IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.attr().matchedTriples());
            List<IndexedSet<Triple>> components = getCartesianComponents(full);
            assert !components.isEmpty();
            if (components.size() == 1) {
                query.attr().setJoinConnected(true);
                return new ComponentNode(query);
            }

            List<SPARQLFilter> pending = new ArrayList<>(query.attr().filters().size());
            List<PlanNode> componentNodes = new ArrayList<>();
            for (IndexedSet<Triple> component : components) {
                MutableCQuery componentQuery = new MutableCQuery(query);
                componentQuery.removeIf(t -> !component.contains(t));
                componentQuery.removeModifierIf(
                        m -> !(m instanceof SPARQLFilter) && !(m instanceof ValuesModifier));
                pending.addAll(componentQuery.sanitizeFiltersStrict());
                componentQuery.attr().setJoinConnected(true);
                componentNodes.add(new ComponentNode(componentQuery));
            }
            CartesianNode root = new CartesianNode(componentNodes);
            pending.forEach(root::addFilter);
            return root;
        }
    }

    @VisibleForTesting
    @Nonnull List<IndexedSet<Triple>> getCartesianComponents(@Nonnull IndexedSet<Triple> triples) {
        List<IndexedSet<Triple>> components = new ArrayList<>();

        IndexedSubset<Triple> visited = triples.emptySubset(), component = triples.emptySubset();
        ArrayDeque<Triple> stack = new ArrayDeque<>(triples.size());
        for (Triple start : triples) {
            if (visited.contains(start))
                continue;
            component.clear();
            stack.push(start);
            while (!stack.isEmpty()) {
                Triple triple = stack.pop();
                if (visited.add(triple)) {
                    component.add(triple);
                    for (Triple next : triples) {
                        if (!visited.contains(next) && hasJoin(triple, next))
                            stack.push(next);
                    }
                }
            }
            components.add(IndexedSet.fromDistinct(component));
        }
        return components;
    }

    private boolean hasJoin(@Nonnull Triple a, @Nonnull Triple b) {
        return  (a.getSubject().isVar()   && b.contains(a.getSubject()  )) ||
                (a.getPredicate().isVar() && b.contains(a.getPredicate())) ||
                (a.getObject().isVar()    && b.contains(a.getObject()   ));
    }
}
