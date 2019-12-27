package br.ufsc.lapesd.riefederator.federation.tree;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;

public class TreeUtils {
    public static boolean isTree(@Nonnull PlanNode node) {
        Map<PlanNode, Boolean> visited = new IdentityHashMap<>();
        Queue<PlanNode> queue = new ArrayDeque<>();
        queue.add(node);
        while (!queue.isEmpty()) {
            PlanNode n = queue.remove();
            if (visited.put(n, true) != null)
                return false; //cycle found
            queue.addAll(n.getChildren());
        }
        return true; // no cycle found
    }

    public static Iterator<PlanNode> iterateDepthLeft(@Nonnull PlanNode root) {
        if (TreeUtils.class.desiredAssertionStatus())
            Preconditions.checkArgument(isTree(root), "plan is not a tree");
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

    public static  @Nonnull Stream<PlanNode> streamDepthLeft(@Nonnull PlanNode root) {
        return StreamSupport.stream(spliteratorUnknownSize(iterateDepthLeft(root),
                DISTINCT | NONNULL), false);
    }
}
