package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.FreeQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.hasIntersect;

public class NaiveOuterPlanner implements OuterPlanner {
    private static final Logger logger = LoggerFactory.getLogger(NaiveOuterPlanner.class);
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
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.OUT_PLAN_MS.createThreadSampler(performance)) {
            Set<RefEquals<Op>> shared = TreeUtils.findSharedNodes(tree  );
            tree = flatten(tree, shared);
            assert isFlattened(tree, shared);
            tree = submergeConjunctions(tree, shared);
            assert isFlattened(tree, shared);
            return TreeUtils.replaceNodes(tree, null, op ->
                op instanceof FreeQueryOp ? handleCartesian((FreeQueryOp)op) : op);
        }
    }

    /**
     * Push any occurence of {@link ConjunctionOp} towards the leaves of the tree until
     * the conjunction stops containing {@link UnionOp} or {@link CartesianOp} nodes.
     *
     * <b>The input tree is not copied and nodes will be directly modified by this operation</b>.
     *
     * Examples:
     * - conj(union(a, b), c) --> union(conj(a, c), conj(b, c))
     * - conj(union(a, b), union(c, d)) --> union(conj(a, c), conj(a, d), conj(b, c), conj(b, d))
     * - conj(a{x}, prod(b{x,y}, c{z,w})) --> prod(conj(a, b), c)
     * - conj(prod(a{x}, b{y}), prod(c{x}, d{z})) --> prod(conj(a, c), b, d)
     *
     * @return The new root of the tree, if it had to be replaced, or the old root.
     */
    private @Nonnull Op submergeConjunctions(@Nonnull Op tree, @Nonnull Set<RefEquals<Op>> shared) {
        if (!(tree instanceof InnerOp)) return tree;
        try (TakenChildren children = ((InnerOp) tree).takeChildren()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(submergeConjunctions(it.next(), shared));
        }
        if (tree instanceof ConjunctionOp) {
            tree = submergeConjunctionOfUnion((ConjunctionOp)tree, shared);
            if (tree instanceof ConjunctionOp)
                tree = submergeConjunctionOfProduct((ConjunctionOp)tree, shared);
        }
        return tree;
    }


    private @Nullable FreeQueryOp getQuery(@Nonnull Op parent,
                                           @Nonnull Set<RefEquals<Op>> shared) {
        assert parent.getChildren().stream().filter(c -> c instanceof FreeQueryOp
                                                && !shared.contains(RefEquals.of(c))).count() == 1;
        for (Op child : parent.getChildren()) {
            if (child instanceof FreeQueryOp && !shared.contains(RefEquals.of(child)))
                return (FreeQueryOp) child;
        }
        return null;
    }

    private @Nonnull Op submergeConjunctionOfProduct(@Nonnull ConjunctionOp parent,
                                                     @Nonnull Set<RefEquals<Op>> shared) {
        // C(P(a{x}, b{y}), P(c{x}, d{z}), U(e, f), g{x}) --> C(P(acg, b, d), U(e, f))
        FreeQueryOp queryOp = getQuery(parent, shared);
        if (queryOp == null) return parent;
        Set<String> queryVars = queryOp.getResultVars();
        MutableCQuery query = queryOp.getQuery();

        boolean hasExtra = false;
        Set<RefEquals<Op>> merged = null;
        for (Op child : parent.getChildren()) {
            if (child instanceof CartesianOp && child.modifiers().isEmpty()) {
                for (Op grandchild : child.getChildren()) {
                    if (grandchild instanceof FreeQueryOp
                            && hasIntersect(grandchild.getResultVars(), queryVars)) {
                        query.mergeWith(((FreeQueryOp) grandchild).getQuery());
                        if (merged == null) merged = new HashSet<>();
                        merged.add(RefEquals.of(grandchild));
                    }
                }
            } else if (child != queryOp) {
                hasExtra = true;
            }
        }
        if (merged == null)
            return parent; // no change
        return raiseProduct(parent, queryOp, hasExtra, merged);
    }

    private @Nonnull Op raiseProduct(@Nonnull ConjunctionOp parent, FreeQueryOp queryOp,
                                     boolean hasExtra, @Nonnull Set<RefEquals<Op>> merged) {
        ConjunctionOp.Builder conjBuilder = hasExtra ? ConjunctionOp.builder() : null;
        CartesianOp.Builder prodBuilder = CartesianOp.builder().add(queryOp);
        for (Op child : parent.getChildren()) {
            if (child instanceof CartesianOp && child.modifiers().isEmpty()) {
                for (Op grandchild : child.getChildren()) {
                    if (!merged.contains(RefEquals.of(grandchild)))
                        prodBuilder.add(grandchild);
                }
            } else if (child != queryOp) {
                assert conjBuilder != null;
                conjBuilder.add(child);
            }
        }
        parent.detachChildren();
        Op prod = prodBuilder.build();
        return conjBuilder != null ? conjBuilder.add(prod).build() : prod;
    }

    private @Nonnull Op submergeConjunctionOfUnion(@Nonnull ConjunctionOp parent,
                                                   @Nonnull Set<RefEquals<Op>> shared) {
        // C(U(a{x}, b{x,y}, c{y}), P(d, e), f) --> C(U(af, bf, c), P(d, e))
        FreeQueryOp outerQueryOp = getQuery(parent, shared);
        if (outerQueryOp == null) return parent; // no op to push into union
        Set<String> outerVars = outerQueryOp.getResultVars();

        boolean hasExtra = false;
        HashSet<RefEquals<Op>> matches = new HashSet<>();
        for (Op child : parent.getChildren()) {
            if (child instanceof UnionOp) {
                for (Op grandChild : child.getChildren()) {
                    if (hasIntersect(grandChild.getResultVars(), outerVars)) {
                        RefEquals<Op> ref = RefEquals.of(grandChild);
                        if (!(grandChild instanceof FreeQueryOp) || shared.contains(ref))
                            return parent; //there is a inner node to which we cannot merge, abort
                        matches.add(ref);
                    }
                }
            } else if (child != outerQueryOp) {
                hasExtra = true;
            }
        }
        assert !matches.isEmpty() : "Should've aborted earlier or non-joinable conjunction";

        return raiseUnion(parent, outerQueryOp, hasExtra, matches);
    }

    private @Nonnull Op raiseUnion(@Nonnull ConjunctionOp parent, @Nonnull FreeQueryOp outerQueryOp,
                                   boolean hasExtra, @Nonnull HashSet<RefEquals<Op>> matches) {
        MutableCQuery outerQuery = outerQueryOp.getQuery();
        ConjunctionOp.Builder conjBuilder = hasExtra ? ConjunctionOp.builder() : null;
        UnionOp.Builder unionBuilder = UnionOp.builder();
        boolean makeDistinct = true, requireDistinct = false;
        for (Op child : parent.getChildren()) {
            if (child instanceof UnionOp) {
                Distinct distinct = child.modifiers().distinct();
                makeDistinct &= distinct != null;
                requireDistinct |= distinct != null && distinct.isRequired();
                for (Op grandchild : child.getChildren()) {
                    if (matches.contains(RefEquals.of(grandchild)))
                        ((FreeQueryOp)grandchild).getQuery().mergeWith(outerQuery);
                    unionBuilder.add(grandchild);
                }
            } else if (child != outerQueryOp) {
                assert  conjBuilder != null;
                conjBuilder.add(child);
            }
        }
        parent.detachChildren();
        if (requireDistinct)
            unionBuilder.add(Distinct.REQUIRED);
        else if (makeDistinct)
            unionBuilder.add(Distinct.ADVISED);
        Op union = unionBuilder.build();
        return (conjBuilder != null ? conjBuilder.add(union).build() : union);
    }

    private boolean isFlattened(@Nonnull Op op, @Nonnull Set<RefEquals<Op>> shared) {
        List<Op> list = op.getChildren();
        if (op instanceof ConjunctionOp
                && list.stream().anyMatch(c -> c instanceof ConjunctionOp
                                                && !shared.contains(RefEquals.of(c)))) {
            return false;
        }
        if (list.size() == 1 && !shared.contains(RefEquals.of(op)))
            return false; // tree should've been replaced with the single child
        if ((op instanceof CartesianOp || op instanceof ConjunctionOp)
                && list.stream().filter(c -> c instanceof FreeQueryOp
                                          && !shared.contains(RefEquals.of(c))).count() > 1) {
            return false; //all non-shared query nodes under ×, or . should've been merged
        }
        // check same-class nesting:
        if (op instanceof CartesianOp && list.stream().anyMatch(CartesianOp.class::isInstance))
            return false;
        if (op instanceof ConjunctionOp && list.stream().anyMatch(ConjunctionOp.class::isInstance))
            return false;
        if (op instanceof UnionOp && list.stream().anyMatch(UnionOp.class::isInstance))
            return false;
        return list.stream().allMatch(c -> isFlattened(c, shared)); //recurse
    }

    private Op flatten(@Nonnull Op tree, @Nonnull Set<RefEquals<Op>> sharedNodes) {
        if (!(tree instanceof InnerOp)) return tree;
        InnerOp io = (InnerOp) tree;
        boolean ioShared = sharedNodes.contains(RefEquals.of(io));
        try (TakenChildren children = io.takeChildren()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(flatten(it.next(), sharedNodes));
            if (io instanceof ConjunctionOp || io instanceof CartesianOp || io instanceof UnionOp)
                flattenSameClass(sharedNodes, io, children);
            if (io instanceof ConjunctionOp || io instanceof CartesianOp)
                mergeQueryOps(sharedNodes, children);
            if (children.size() == 1 && !ioShared)
                return children.get(0); // replace conj(a) with a
        }
        return io; //changed in-place
    }

    private void flattenSameClass(@Nonnull Set<RefEquals<Op>> shared, InnerOp parent,
                                  TakenChildren children) {
        Class<? extends InnerOp> parentClass = parent.getClass();
        for (int i = 0, size = children.size(); i < size; i++) {
            Op c = children.get(i);
            if (c.getClass().equals(parentClass) && !shared.contains(RefEquals.of(c))) {
                Iterator<Op> it = c.getChildren().iterator();
                if (it.hasNext()) {
                    children.set(i, it.next());
                    while (it.hasNext())
                        children.add(++i, it.next());
                    size += c.getChildren().size() - 1;
                    parent.modifiers().mergeWith(c.modifiers(), parent.getResultVars(),
                            c.getResultVars());
                } else {
                    children.remove(i--);
                    --size;
                }
            }
        }
    }

    private void mergeQueryOps(@Nonnull Set<RefEquals<Op>> sharedNodes, TakenChildren children) {
        FreeQueryOp queryOp = null;
        for (Iterator<Op> it = children.iterator(); it.hasNext(); ) {
            Op child = it.next();
            if (child instanceof FreeQueryOp && !sharedNodes.contains(RefEquals.of(child))) {
                if (queryOp == null) {
                    queryOp = (FreeQueryOp) child;
                } else {
                    MutableCQuery q = queryOp.getQuery();
                    if (q.mergeWith(((FreeQueryOp) child).getQuery()))
                        queryOp.setQuery(q);
                    else
                        logger.warn("Query appears twice in a conjunction: {}", q);
                    it.remove();
                }
            }
        }
    }

    private @Nonnull Op handleCartesian(@Nonnull FreeQueryOp queryOp) {
        MutableCQuery query = queryOp.getQuery();
        if (query.isEmpty())
            return new EmptyOp(query);
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
            componentNodes.add(new FreeQueryOp(componentQuery));
        }
        CartesianOp root = new CartesianOp(componentNodes);
        // add all filters that are relevant to some node but can't be evaluated there
        query.getModifiers().filters().stream()
                .filter(f -> componentNodes.stream()
                        .anyMatch(n -> !n.modifiers().contains(f)
                                    && hasIntersect(n.getAllVars(), f.getVarTermNames())))
                .forEach(root.modifiers()::add);
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
