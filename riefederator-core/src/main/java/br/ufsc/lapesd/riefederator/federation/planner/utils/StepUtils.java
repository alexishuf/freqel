package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.*;

public class StepUtils {
    public static void exposeFilterVars(@Nonnull Op op) {
        Projection projection = op.modifiers().projection();
        if (projection != null) {
            Set<String> ok = projection.getVarNames();
            Set<String> required = null;
            for (SPARQLFilter filter : op.modifiers().filters()) {
                for (String var : filter.getVarTermNames()) {
                    if (!ok.contains(var))
                        (required == null ? required = new HashSet<>() : required).add(var);
                }
            }
            if (required != null) {
                required.addAll(ok);
                op.modifiers().add(Projection.of(required));
            }
        }
    }

    public static @Nonnull Op planConjunction(@Nonnull Collection<Op> nodes,
                                              @Nonnull JoinOrderPlanner joinOrderPlanner) {
        IndexedSet<Op> set = IndexedSet.fromDistinct(nodes);
        JoinGraph jg = new JoinGraph(set);
        IndexedSubset<Op> visited = set.emptySubset();
        List<Op> trees = new ArrayList<>();
        ArrayDeque<Op> stack = new ArrayDeque<>();
        for (Op core : set) {
            if (!visited.contains(core))
                trees.add(getJoinTree(core, jg, visited, stack, joinOrderPlanner));
        }
        assert !trees.isEmpty();
        Op op = trees.size() == 1 ? trees.get(0) : new CartesianOp(trees);
        StepUtils.exposeFilterVars(op);
        return op;
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
