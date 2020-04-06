package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;

public class TreeUtils {
    public static boolean isTree(@Nonnull PlanNode node) {
        return isTree(node, false);
    }
    public static boolean isTree(@Nonnull PlanNode node, boolean forgiveQueryNodes) {
        Map<PlanNode, Boolean> visited = new IdentityHashMap<>();
        Queue<PlanNode> queue = new ArrayDeque<>();
        queue.add(node);
        while (!queue.isEmpty()) {
            PlanNode n = queue.remove();
            if (!forgiveQueryNodes || !(n instanceof QueryNode)) {
                if (visited.put(n, true) != null)
                    return false; //cycle found
            }
            queue.addAll(n.getChildren());
        }
        return true; // no cycle found
    }

    private static class AcyclicOp {
        @Nonnull PlanNode node;
        boolean entering;

        public AcyclicOp(@Nonnull PlanNode node, boolean entering) {
            this.node = node;
            this.entering = entering;
        }
        public static @Nonnull
        AcyclicOp entering(@Nonnull PlanNode node) {
            return new AcyclicOp(node, true);
        }
        public static @Nonnull
        AcyclicOp leaving(@Nonnull PlanNode node) {
            return new AcyclicOp(node, false);
        }

        @Override
        public String toString() {
            return String.format("%s %s", entering ? "ENTER" : "LEAVE", node);
        }
    }

    public static boolean isAcyclic(@Nonnull PlanNode root) {
        Set<PlanNode> open = new HashSet<>();
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

    public static @Nonnull Iterator<PlanNode> iteratePreOrder(@Nonnull PlanNode root) {
        if (TreeUtils.class.desiredAssertionStatus())
            Preconditions.checkArgument(isTree(root, true), "Plan is not a tree!");
        ArrayDeque<PlanNode> stack = new ArrayDeque<>();
        stack.push(root);
        return new Iterator<PlanNode>() {
            @Override
            public boolean hasNext() {
                return !stack.isEmpty();
            }

            @Override
            public PlanNode next() {
                PlanNode node = stack.pop();
                List<PlanNode> list = node.getChildren();
                ListIterator<PlanNode> it = list.listIterator(list.size());
                while (it.hasPrevious())
                    stack.push(it.previous());
                return node;
            }
        };
    }

    public static  @Nonnull Stream<PlanNode> streamPreOrder(@Nonnull PlanNode root) {
        return StreamSupport.stream(spliteratorUnknownSize(iteratePreOrder(root),
                DISTINCT | NONNULL), false);
    }

    static @Nonnull <T, I>
    Set<T> intersect(@Nonnull Collection<I> input,
                     @Nonnull Function<I, ? extends Collection<T>> getter,
                     @Nullable AtomicBoolean dropped) {
        boolean drop = false;
        Iterator<I> it = input.iterator();
        Set<T> result = new HashSet<>(it.hasNext() ? getter.apply(it.next()) : emptySet());
        while (it.hasNext()) {
            Collection<T> values = getter.apply(it.next());
            drop |= result.retainAll(values);
            if (dropped != null && !drop && !result.containsAll(values))
                drop = true;
        }
        if (dropped != null) dropped.set(drop);
        return result.isEmpty() ? emptySet() : result;
    }

    public static @Nonnull <T, I>
    Set<T> union(@Nonnull Collection<I> input,
                 @Nonnull Function<I, ? extends Collection<T>> getter) {
        Set<T> set = new HashSet<>();
        for (I i : input) set.addAll(getter.apply(i));
        return set;
    }

    public static @Nonnull <T> Set<T> union(@Nonnull Collection<T> a, @Nonnull Collection<T> b) {
        HashSet<T> set = new HashSet<>(a.size() + b.size());
        set.addAll(a);
        set.addAll(b);
        return set;
    }

    public static @Nonnull <T> Set<T> union(@Nonnull Collection<T> a, @Nonnull Collection<T> b,
                                            @Nonnull Collection<T> c) {
        HashSet<T> set = new HashSet<>(a.size() + b.size() + c.size());
        set.addAll(a);
        set.addAll(b);
        set.addAll(c);
        return set;
    }


    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> left, Collection<T> right) {
        Set<T> result = new HashSet<>(left.size() < right.size() ? left : right);
        result.retainAll(left.size() < right.size() ? right : left);
        return result;
    }
    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> a,
                                                @Nonnull Collection<T> b,
                                                @Nonnull Collection<T> c) {
        Set<T> result = new HashSet<>(a);
        result.retainAll(b);
        result.retainAll(c);
        return result;
    }

    public static @Nonnull <T, I>
    Set<T> intersect(@Nonnull Collection<I> input,
                     @Nonnull Function<I, ? extends Collection<T>> getter) {
        if (input.isEmpty())
            return emptySet();
        Iterator<I> it = input.iterator();
        Set<T> set = new HashSet<>(getter.apply(it.next()));
        while (it.hasNext())
            set.retainAll(getter.apply(it.next()));
        return set;
    }

    public static @Nonnull <T> Set<T> setMinus(@Nonnull Collection<T> left,
                                               @Nonnull Collection<T> right) {
        HashSet<T> set = new HashSet<>(left);
        set.removeAll(right);
        return set;
    }

    public static @Nonnull List<PlanNode> childrenIfMulti(@Nonnull PlanNode node) {
        return node instanceof MultiQueryNode ? Collections.unmodifiableList(node.getChildren())
                                              : singletonList(node);
    }

    public static  @Nonnull PlanNode cleanEquivalents(@Nonnull PlanNode node) {
        return cleanEquivalents(node, Comparator.comparing(PlanNode::hashCode));
    }

    public static  @Nonnull PlanNode cleanEquivalents(@Nonnull PlanNode node,
                                                      @Nonnull Comparator<PlanNode> comparator) {
        node = flattenMultiQuery(node);
        if (!(node instanceof MultiQueryNode)) return node;

        ListMultimap<Set<Triple>, QueryNode> mm;
        mm = MultimapBuilder.hashKeys().arrayListValues().build();
        List<PlanNode> children = node.getChildren();
        for (PlanNode child : children) {
            if (child instanceof QueryNode)
                mm.put(((QueryNode) child).getQuery().getSet(), (QueryNode) child);
        }

        BitSet mkd = new BitSet(children.size());
        for (Set<Triple> key : mm.keySet()) {
            List<QueryNode> list = mm.get(key);
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
            MultiQueryNode.Builder builder = MultiQueryNode.builder();
            for (int i = mkd.nextSetBit(0); i >= 0; i = mkd.nextSetBit(i+1))
                builder.add(children.get(i));
            return builder.buildIfMulti();
        } else {
            return node;
        }
    }

    public static  @Nonnull PlanNode flattenMultiQuery(@Nonnull PlanNode node) {
        if (!(node instanceof MultiQueryNode)) return node;

        if (node.getChildren().size() == 1) {
            return node.getChildren().get(0);
        } else if (node.getChildren().stream().anyMatch(MultiQueryNode.class::isInstance)) {
            MultiQueryNode.Builder builder = MultiQueryNode.builder();
            node.getChildren().forEach(c -> flattenMultiQuery(c, builder));
            return builder.buildIfMulti();
        } else {
            return node;
        }
    }

    private static void flattenMultiQuery(@Nonnull PlanNode node,
                                   @Nonnull MultiQueryNode.Builder builder) {
        if (node instanceof MultiQueryNode)
            node.getChildren().forEach(c -> flattenMultiQuery(c, builder));
        else
            builder.add(node);
    }
}
