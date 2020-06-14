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
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

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
            IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.getMatchedTriples());
            List<IndexedSet<Triple>> components = getCartesianComponents(full);
            assert !components.isEmpty();
            if (components.size() == 1) {
                query.setJoinConnected(true);
                return new ComponentNode(query);
            }

            Set<SPARQLFilter> filters = query.getModifiers().stream()
                    .filter(SPARQLFilter.class::isInstance)
                    .map(m -> (SPARQLFilter) m).collect(toSet());
            List<PlanNode> componentNodes = new ArrayList<>();
            for (IndexedSet<Triple> component : components) {
                CQuery.WithBuilder builder = CQuery.with(component);
                Set<String> allVars = builder.getList().stream().flatMap(Triple::stream)
                        .filter(Term::isVar).map(v -> v.asVar().getName()).collect(toSet());
                for (Modifier modifier : query.getModifiers()) {
                    if (modifier instanceof Distinct) {
                        builder.modifier(modifier);
                    } else if (modifier instanceof SPARQLFilter) {
                        SPARQLFilter filter = (SPARQLFilter) modifier;
                        if (allVars.containsAll(filter.getVarTermNames())) {
                            builder.modifier(modifier);
                            filters.remove(filter);
                        }
                    }
                }
                builder.copyAnnotations(query);
                builder.setJoinConnected(true);
                componentNodes.add(new ComponentNode(builder.build()));
            }
            CartesianNode root = new CartesianNode(componentNodes);
            filters.forEach(root::addFilter);
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
