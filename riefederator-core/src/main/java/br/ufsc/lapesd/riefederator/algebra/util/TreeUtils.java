package br.ufsc.lapesd.riefederator.algebra.util;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonList;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;

public class TreeUtils {
    public static boolean isTree(@Nonnull Op node) {
        return isTree(node, false);
    }
    public static boolean isTree(@Nonnull Op node, boolean forgiveQueryNodes) {
        Map<Op, Boolean> visited = new IdentityHashMap<>();
        Queue<Op> queue = new ArrayDeque<>();
        queue.add(node);
        while (!queue.isEmpty()) {
            Op n = queue.remove();
            if (!forgiveQueryNodes || !(n instanceof QueryOp)) {
                if (visited.put(n, true) != null)
                    return false; //cycle found
            }
            queue.addAll(n.getChildren());
        }
        return true; // no cycle found
    }

    /**
     * Creates a copy of the whole given {@link Op} tree.
     */
    public static @Nonnull Op deepCopy(@Nonnull Op root) {
        Op copy = root.flatCopy();
        if (copy instanceof InnerOp) {
            try (TakenChildren children = ((InnerOp) copy).takeChildren()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); ) {
                    it.set(deepCopy(it.next()));
                }
            }
        }
        return copy;
    }

    /**
     * Replace nodes in the tree spanning from the given root whenever map returns
     * a non-null {@link Op} different from the {@link Op} given as argument.
     *
     * For inner nodes, a node that has children replaced remains in the tree, only its
     * children are replaced. Note that the map function may receive as arguments {@link Op}
     * instances previously output by the function itself, as a the replacement node will be
     * visited in place of the replaced node.
     *
     * If the root {@link Op} happens to be replaced, the replacement will be returned by
     * this call, otherwise the root {@link Op} (possibly with changed children) is returned.
     *
     * @param root the root of a {@link Op} tree
     * @param cardinality if non-null recomputes the cardinality of every node that
     *                    had children replaced.
     * @param map function that maps original nodes to replacements (or returns the original
     *            node or null if it should not be replaced).
     * @return the tree root (only differs from the root parameter if that node is
     *         replaced by the map function).
     */
    @CheckReturnValue
    public static @Nonnull Op replaceNodes(@Nonnull Op root,
                                           @Nullable InnerCardinalityComputer cardinality,
                                           @Nonnull Function<Op, Op> map) {
        Function<Op, Op> memoized = CollectionUtils.memoize(n -> {
            Op replacement = map.apply(n);
            return replacement == null ? n : replacement;
        }, RefEquals::of);
        ArrayDeque<Op> stack = new ArrayDeque<>();
        root = memoized.apply(root);
        if (root instanceof InnerOp)
            stack.push(root);
        while (!stack.isEmpty()) {
            InnerOp op = (InnerOp)stack.pop();
            boolean change = false;
            try (TakenChildren children = op.takeChildren()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); ) {
                    Op child = it.next();
                    Op replacement = memoized.apply(child);
                    change |= replacement != child;
                    it.set(replacement);
                    if (replacement instanceof InnerOp)
                        stack.push(replacement);
                }
                children.setHasChanges(change);
            }
            if (change && cardinality != null)
                op.setCardinality(cardinality.compute(op));
        }
        return root;
    }

    public static boolean addBoundModifiers(@Nonnull Collection<Modifier> out,
                                            @Nonnull Collection<Modifier> in,
                                            @Nonnull Solution solution) {
        boolean change = false;
        for (Modifier modifier : in) {
            if (modifier instanceof SPARQLFilter) {
                change |= out.add(((SPARQLFilter) modifier).bind(solution));
            } else if (modifier instanceof Projection) {
                Projection p = (Projection) modifier;
                HashSet<String> set = new HashSet<>(p.getVarNames());
                if (set.removeAll(solution.getVarNames())) {
                    Projection p2 = new Projection(set, p.isRequired());
                    change |= out.add(p2);
                } else {
                    change |= out.add(p);
                }
            } else {
                change |= out.add(modifier);
            }
        }
        return change;
    }

    private static class AcyclicOp {
        @Nonnull Op node;
        boolean entering;

        public AcyclicOp(@Nonnull Op node, boolean entering) {
            this.node = node;
            this.entering = entering;
        }
        public static @Nonnull
        AcyclicOp entering(@Nonnull Op node) {
            return new AcyclicOp(node, true);
        }
        public static @Nonnull
        AcyclicOp leaving(@Nonnull Op node) {
            return new AcyclicOp(node, false);
        }

        @Override
        public String toString() {
            return String.format("%s %s", entering ? "ENTER" : "LEAVE", node);
        }
    }

    public static boolean isAcyclic(@Nonnull Op root) {
        Set<Op> open = new HashSet<>();
        ArrayDeque<AcyclicOp> stack = new ArrayDeque<>();
        stack.push(AcyclicOp.entering(root));
        while (!stack.isEmpty()) {
            AcyclicOp operation = stack.pop();
            if (operation.entering) {
                if (!open.add(operation.node)) {
                    return false; //cycle detected!
                } else {
                    stack.push(AcyclicOp.leaving(operation.node));
                    operation.node.getChildren().forEach(c -> stack.push(AcyclicOp.entering(c)));
                }
            } else {
                assert open.contains(operation.node);
                open.remove(operation.node);
            }
        }
        return true;
    }

    public static @Nonnull Iterator<Op> iteratePreOrder(@Nonnull Op root) {
        if (TreeUtils.class.desiredAssertionStatus())
            Preconditions.checkArgument(isAcyclic(root), "Plan is not a tree!");
        ArrayDeque<Op> stack = new ArrayDeque<>();
        stack.push(root);
        return new Iterator<Op>() {
            @Override
            public boolean hasNext() {
                return !stack.isEmpty();
            }

            @Override
            public Op next() {
                Op node = stack.pop();
                List<Op> list = node.getChildren();
                ListIterator<Op> it = list.listIterator(list.size());
                while (it.hasPrevious())
                    stack.push(it.previous());
                return node;
            }
        };
    }

    public static  @Nonnull Stream<Op> streamPreOrder(@Nonnull Op root) {
        return StreamSupport.stream(spliteratorUnknownSize(iteratePreOrder(root),
                DISTINCT | NONNULL), false);
    }

    public static @Nonnull Set<RefEquals<Op>> findSharedNodes(@Nonnull Op tree) {
        ArrayDeque<Op> queue = new ArrayDeque<>();
        queue.add(tree);
        Map<RefEquals<Op>, Integer> map = new HashMap<>();
        while (!queue.isEmpty()) {
            Op op = queue.remove();
            RefEquals<Op> key = new RefEquals<>(op);
            if (map.put(key, map.getOrDefault(key, 0) + 1) == null)
                queue.addAll(op.getChildren());
        }
        map.entrySet().removeIf(e -> e.getValue() <= 1);
        return map.keySet();
    }

    public static @Nonnull List<Op> childrenIfMulti(@Nonnull Op node) {
        return node instanceof UnionOp ? Collections.unmodifiableList(node.getChildren())
                                              : singletonList(node);
    }

    public static  @Nonnull Op cleanEquivalents(@Nonnull Op node) {
        return cleanEquivalents(node, Comparator.comparing(Op::hashCode));
    }

    public static  @Nonnull Op cleanEquivalents(@Nonnull Op node,
                                                @Nonnull Comparator<Op> comparator) {
        node = flattenMultiQuery(node);
        if (!(node instanceof UnionOp)) return node;

        ListMultimap<CQuery, QueryOp> mm;
        mm = MultimapBuilder.hashKeys().arrayListValues().build();
        List<Op> children = node.getChildren();
        for (Op child : children) {
            if (child instanceof QueryOp)
                mm.put(((QueryOp) child).getQuery(), (QueryOp) child);
        }

        BitSet mkd = new BitSet(children.size());
        for (CQuery key : mm.keySet()) {
            List<QueryOp> list = mm.get(key);
            for (int i = 0; i < list.size(); i++) {
                if (mkd.get(i)) continue;
                TPEndpoint outer = list.get(i).getEndpoint();
                for (int j = i+1; j < list.size(); j++) {
                    if (mkd.get(j)) continue;
                    TPEndpoint inner = list.get(j).getEndpoint();
                    if (outer.isAlternative(inner) || inner.isAlternative(outer)) {
                        int worst = comparator.compare(list.get(i), list.get(j)) <= 0 ? j : i;
                        mkd.set(worst); //mark for removal
                    }
                }
            }
        }

        if (mkd.cardinality() > 0) {
            // transform marked "for removal" into marked "for survival"
            mkd.flip(0, children.size());
            UnionOp.Builder builder = UnionOp.builder();
            for (int i = mkd.nextSetBit(0); i >= 0; i = mkd.nextSetBit(i+1))
                builder.add(children.get(i));
            return builder.build();
        } else {
            return node;
        }
    }

    public static @Nonnull Cardinality estimate(@Nonnull Op node,
                                                @Nonnull CardinalityEnsemble ensemble,
                                                @Nonnull CardinalityAdder adder) {
        if (node instanceof UnionOp) {
            for (Op child : node.getChildren())
                child.setCardinality(estimate(child, ensemble, adder));
            return node.getChildren().stream().map(Op::getCardinality)
                                     .reduce(adder).orElse(Cardinality.EMPTY);
        } else if (node instanceof QueryOp) {
            QueryOp qn = (QueryOp) node;
            return ensemble.estimate(qn.getQuery(), qn.getEndpoint());
        }
        return node.getCardinality();
    }

    public static  @Nonnull Op flattenMultiQuery(@Nonnull Op node) {
        if (!(node instanceof UnionOp)) return node;

        if (node.getChildren().size() == 1) {
            return node.getChildren().get(0);
        } else if (node.getChildren().stream().anyMatch(UnionOp.class::isInstance)) {
            UnionOp.Builder builder = UnionOp.builder();
            node.getChildren().forEach(c -> flattenMultiQuery(c, builder));
            return builder.build();
        } else {
            return node;
        }
    }

    private static void flattenMultiQuery(@Nonnull Op node,
                                   @Nonnull UnionOp.Builder builder) {
        if (node instanceof UnionOp)
            node.getChildren().forEach(c -> flattenMultiQuery(c, builder));
        else
            builder.add(node);
    }

    public static void nameNodes(@Nonnull Op root) {
        ArrayDeque<Op> stack = new ArrayDeque<>();
        stack.push(root);
        Set<Op> visited = new HashSet<>();
        int joins = 0, queryNodes = 0, mqNodes = 0, cartesianNodes = 0, emptyNodes = 0, oNodes = 0;
        while (!stack.isEmpty()) {
            Op node = stack.pop();
            if (!visited.add(node))
                continue;
            node.getChildren().forEach(stack::push);
            if (node instanceof JoinOp) {
                ++joins;
                node.setName("Join-"+joins);
            } else if (node instanceof QueryOp) {
                ++queryNodes;
                node.setName("Query-"+queryNodes);
            } else if (node instanceof UnionOp) {
                ++mqNodes;
                node.setName("MultiQuery-"+mqNodes);
            } else if (node instanceof CartesianOp) {
                ++cartesianNodes;
                node.setName("Cartesian-"+cartesianNodes);
            } else if (node instanceof EmptyOp) {
                ++emptyNodes;
                node.setName("Empty-"+emptyNodes);
            } else {
                ++oNodes;
                node.setName("Other-"+oNodes);
            }
        }
    }
}