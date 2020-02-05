package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
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
import static java.util.stream.Stream.concat;

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

    public static @Nonnull Set<String> intersectInputs(@Nonnull Collection<PlanNode> collection) {
        return intersect(collection, PlanNode::getInputVars, null);
    }
    public static @Nonnull Set<String> intersectResults(@Nonnull Collection<PlanNode> collection) {
        return intersect(collection, PlanNode::getResultVars, null);
    }

    public static @Nonnull Set<String> intersectInputs(@Nonnull Collection<PlanNode> collection,
                                                @Nonnull AtomicBoolean dropped) {
        return intersect(collection, PlanNode::getInputVars, dropped);
    }
    public static @Nonnull Set<String> intersectResults(@Nonnull Collection<PlanNode> collection,
                                                 @Nonnull AtomicBoolean dropped) {
        return intersect(collection, PlanNode::getResultVars, dropped);
    }

    public static @Nonnull Set<String> unionInputs(@Nonnull Collection<PlanNode> collection) {
        return union(collection, PlanNode::getInputVars);
    }

    public static @Nonnull Set<String> unionResults(@Nonnull Collection<PlanNode> collection) {
        return union(collection, PlanNode::getResultVars);
    }

    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> left, Collection<T> right) {
        Set<T> result = new HashSet<>(left.size() < right.size() ? left : right);
        result.retainAll(left.size() < right.size() ? right : left);
        return result;
    }

    public static @Nonnull <T> Set<T> setMinus(@Nonnull Collection<T> left,
                                               @Nonnull Collection<T> right) {
        HashSet<T> set = new HashSet<>(left);
        set.removeAll(right);
        return set;
    }

    public static @Nonnull Set<String> joinVars(@Nonnull PlanNode l, @Nonnull PlanNode r) {
        return joinVars(l, r, null);
    }

    public static @Nonnull Set<String> joinVars(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                                @Nullable Set<String> pendingIns) {
        Set<String> s = intersect(l.getResultVars(), r.getResultVars());
        Set<String> lIn = l.getInputVars();
        Set<String> rIn = r.getInputVars();
        if (pendingIns != null)
            pendingIns.clear();
        if (l.hasInputs() && r.hasInputs()) {
            if (s.stream().anyMatch(n -> lIn.contains(n) == rIn.contains(n)))
                return emptySet();
        } else if (l.hasInputs()) {
            s.removeIf(n -> !lIn.contains(n));
        } else if (r.hasInputs()) {
            s.removeIf(n -> !rIn.contains(n));
        }
        if (pendingIns != null)
            concat(lIn.stream(), rIn.stream()).filter(n -> !s.contains(n)).forEach(pendingIns::add);
        return s;
    }

    public static @Nonnull List<PlanNode> childrenIfMulti(@Nonnull PlanNode node) {
        return node instanceof MultiQueryNode ? Collections.unmodifiableList(node.getChildren())
                                              : singletonList(node);
    }

    public static  @Nonnull PlanNode cleanEquivalents(@Nonnull PlanNode node) {
        node = flattenMultiQuery(node);
        if (!(node instanceof MultiQueryNode)) return node;

        ListMultimap<Set<Triple>, QueryNode> mm;
        mm = MultimapBuilder.hashKeys().arrayListValues().build();
        List<PlanNode> children = node.getChildren();
        for (PlanNode child : children) {
            if (child instanceof QueryNode)
                mm.put(((QueryNode) child).getQuery().getSet(), (QueryNode) child);
        }

        BitSet marked = new BitSet(children.size());
        for (Set<Triple> key : mm.keySet()) {
            List<QueryNode> list = mm.get(key);
            for (int i = 0; i < list.size(); i++) {
                if (marked.get(i)) continue;
                TPEndpoint outer = list.get(i).getEndpoint();
                for (int j = i+1; j < list.size(); j++) {
                    if (marked.get(j)) continue;
                    TPEndpoint inner = list.get(j).getEndpoint();
                    if (outer.isAlternative(inner) || inner.isAlternative(outer))
                        marked.set(j);
                }
            }
        }

        if (marked.cardinality() > 0) {
            MultiQueryNode.Builder builder = MultiQueryNode.builder();
            for (int i = marked.nextSetBit(0); i >= 0; i = marked.nextSetBit(i+1))
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
