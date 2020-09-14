package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StepUtils {
    public static @Nonnull Op planConjunction(@Nonnull Collection<Op> nodes,
                                              @Nonnull JoinOrderPlanner joinOrderPlanner) {
        IndexedSet<Op> set = IndexedSet.fromRefDistinct(nodes);
        JoinGraph jg = new JoinGraph(set);
        IndexedSubset<Op> visited = set.emptySubset();
        List<Op> trees = new ArrayList<>();
        ArrayDeque<Op> stack = new ArrayDeque<>();
        for (Op core : set) {
            if (!visited.contains(core))
                trees.add(getJoinTree(core, jg, visited, stack, joinOrderPlanner));
        }
        assert !trees.isEmpty();
        return trees.size() == 1 ? trees.get(0) : new CartesianOp(trees);
    }

    private static @Nonnull Op getJoinTree(@Nonnull Op core, @Nonnull JoinGraph jg,
                                           @Nonnull IndexedSubset<Op> visited,
                                           @Nonnull ArrayDeque<Op> stack,
                                           @Nonnull JoinOrderPlanner joinOrderPlanner) {
        IndexedSet<Op> nodes = jg.getNodes();
        IndexedSubset<Op> component = nodes.emptySubset();
        stack.clear();
        stack.push(core);
        while (!stack.isEmpty()) {
            Op op = stack.pop();
            if (!component.add(op)) continue;
            jg.forEachNeighbor(op, (ji, next) -> stack.push(next));
        }
        visited.addAll(component);
        return joinOrderPlanner.plan(jg, component);
    }
}
