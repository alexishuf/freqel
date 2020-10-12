package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.hasIntersect;

public class CartesianIntroductionStep implements PlannerStep, PlannerShallowStep {
    private static final Logger logger = LoggerFactory.getLogger(CartesianIntroductionStep.class);

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        if (root instanceof InnerOp) {
            try (TakenChildren children = ((InnerOp) root).takeChildren().setNoContentChange()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                    it.set(plan(it.next(), shared));
            }
            return root;
        } else if (root instanceof QueryOp) {
            Op replacement = handleCartesian((QueryOp) root);
            if (replacement != root && shared.contains(root)) {
                logger.warn("Replaced join-disconnected locked QueryOp {} with a CartesianOp " +
                            "which was added to the locked set", root);
                shared.add(replacement);
            }
            return replacement;
        }
        return root;
    }

    @Override public @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared) {
        if (op instanceof QueryOp) {
            Op replacement = handleCartesian((QueryOp) op);
            if (replacement != op && shared.contains(op)) {
                logger.warn("Replaced join-disconnected locked QueryOp {} with a CartesianOp " +
                            "which was added to the locked set", op);
                shared.add(replacement);
            }
            return replacement;
        }
        return op; // no change
    }

    @Override
    public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    /* --- --- --- Internals --- --- ---*/

    private @Nonnull Op handleCartesian(@Nonnull QueryOp queryOp) {
        MutableCQuery query = queryOp.getQuery();
        if (query.isEmpty())
            return new EmptyOp(queryOp);
        IndexSet<Triple> full = FullIndexSet.fromDistinct(query.attr().matchedTriples());
        List<IndexSet<Triple>> components = getCartesianComponents(full);
        assert !components.isEmpty();
        if (components.size() == 1) {
            query.attr().setJoinConnected(true);
            return queryOp;
        }

        List<Op> componentNodes = new ArrayList<>();
        for (IndexSet<Triple> component : components) {
            MutableCQuery componentQuery = new MutableCQuery(query);
            componentQuery.removeIf(t -> !component.contains(t));
            componentQuery.mutateModifiers().removeIf(
                    m -> !(m instanceof SPARQLFilter) && !(m instanceof ValuesModifier));
            componentQuery.sanitizeFiltersStrict();
            componentQuery.attr().setJoinConnected(true);
            componentNodes.add(new QueryOp(componentQuery));
        }
        CartesianOp root = new CartesianOp(componentNodes);
        // add relevant modifiers to the root
        for (Modifier m : queryOp.modifiers()) {
            if (m instanceof SPARQLFilter) {
                SPARQLFilter f = (SPARQLFilter) m;
                boolean pending = componentNodes.stream().anyMatch(n -> !n.modifiers().contains(f)
                        && hasIntersect(n.getAllVars(), f.getVarNames()));
                if (!pending)
                    continue; //if the filter is not pending anywhere, to not add to the root
            } else if (m instanceof ValuesModifier) {
                continue; //these are always added to every component
            }
            root.modifiers().add(m);
        }
        return root;
    }

    @VisibleForTesting
    @Nonnull List<IndexSet<Triple>> getCartesianComponents(@Nonnull IndexSet<Triple> triples) {
        List<IndexSet<Triple>> components = new ArrayList<>();

        IndexSubset<Triple> visited = triples.emptySubset(), component = triples.emptySubset();
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
            components.add(FullIndexSet.fromDistinctCopy(component));
        }
        return components;
    }

    private boolean hasJoin(@Nonnull Triple a, @Nonnull Triple b) {
        return  (a.getSubject().isVar()   && b.contains(a.getSubject()  )) ||
                (a.getPredicate().isVar() && b.contains(a.getPredicate())) ||
                (a.getObject().isVar()    && b.contains(a.getObject()   ));
    }
}
