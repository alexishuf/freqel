package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.hasIntersect;

public class CartesianIntroductionStep implements PlannerStep {
    private static final Logger logger = LoggerFactory.getLogger(CartesianIntroductionStep.class);

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (root instanceof InnerOp) {
            try (TakenChildren children = ((InnerOp) root).takeChildren().setNoContentChange()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                    it.set(plan(it.next(), locked));
            }
            return root;
        } else if (root instanceof QueryOp) {
            Op replacement = handleCartesian((QueryOp) root);
            if (replacement != root && locked.contains(RefEquals.of(root))) {
                logger.warn("Replaced join-disconnected locked QueryOp {} with a CartesianOp " +
                            "which was added to the locked set", root);
                locked.add(RefEquals.of(replacement));
            }
            return replacement;
        }
        return root;
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
        IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.attr().matchedTriples());
        List<IndexedSet<Triple>> components = getCartesianComponents(full);
        assert !components.isEmpty();
        if (components.size() == 1) {
            query.attr().setJoinConnected(true);
            return queryOp;
        }

        List<Op> componentNodes = new ArrayList<>();
        for (IndexedSet<Triple> component : components) {
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
                        && hasIntersect(n.getAllVars(), f.getVarTermNames()));
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
