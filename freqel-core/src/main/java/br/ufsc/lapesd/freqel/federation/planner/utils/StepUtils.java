package br.ufsc.lapesd.freqel.federation.planner.utils;

import br.ufsc.lapesd.freqel.algebra.InnerOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.ArrayJoinGraph;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.JoinGraph;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StepUtils {
    public static @Nonnull Op planConjunction(@Nonnull Collection<Op> nodes,
                                              @Nonnull ModifiersSet parentModifiers,
                                              @Nonnull JoinOrderPlanner joinOrderPlanner) {
        RefIndexSet<Op> set = RefIndexSet.fromRefDistinct(nodes);
        JoinGraph jg = new ArrayJoinGraph(set);
        IndexSubset<Op> visited = set.emptySubset();
        List<Op> trees = new ArrayList<>();
        ArrayDeque<Op> stack = new ArrayDeque<>();
        boolean hasCartesian = false;
        for (Op core : set) {
            if (!visited.contains(core)) {
                Op tree = getJoinTree(core, jg, visited, stack, joinOrderPlanner);
                hasCartesian |= tree instanceof CartesianOp;
                trees.add(tree);
            }
        }
        assert !trees.isEmpty();
        if (trees.size() > 1 && hasCartesian) {
            for (int i = 0, size = trees.size(); i < size; i++) {
                Op tree = trees.get(i);
                if (tree instanceof CartesianOp && tree.modifiers().optional() == null) {
                    trees.remove(i--);
                    trees.addAll(((InnerOp) tree).takeChildren());
                }
            }
        }
        Op root = trees.size() == 1 ? trees.get(0) : new CartesianOp(trees);
        TreeUtils.copyNonFilter(root, parentModifiers);
        return root;
    }

    private static @Nonnull Op getJoinTree(@Nonnull Op core, @Nonnull JoinGraph jg,
                                           @Nonnull IndexSubset<Op> visited,
                                           @Nonnull ArrayDeque<Op> stack,
                                           @Nonnull JoinOrderPlanner joinOrderPlanner) {
        RefIndexSet<Op> nodes = jg.getNodes();
        IndexSubset<Op> component = nodes.emptySubset();
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
