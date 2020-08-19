package br.ufsc.lapesd.riefederator.federation.planner.outer.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.inner.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.outer.OuterPlannerStep;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

public class ConjunctionReplaceStep implements OuterPlannerStep {
    private static final Logger logger = LoggerFactory.getLogger(ConjunctionReplaceStep.class);

    private final @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public ConjunctionReplaceStep(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (!(root instanceof InnerOp))
            return root;
        InnerOp io = (InnerOp) root;
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(plan(it.next(), locked));
        }
        if (root instanceof ConjunctionOp) {
            if (locked.contains(RefEquals.of(root))) {
                assert false;
                logger.error("Locked ConjunctionOp {} MUST be replaced to be have an " +
                             "executable plan! Will ignore its locked status", root);
            }
            return visit((ConjunctionOp) root);
        }
        return root;
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    /* --- --- --- Internals --- --- --- */

    private @Nonnull Op visit(@Nonnull ConjunctionOp parent) {
        JoinGraph jg = new JoinGraph(IndexedSet.fromDistinct(parent.getChildren()));
        IndexedSet<Op> nodes = jg.getNodes();
        IndexedSubset<Op> visited = nodes.emptySubset();
        List<Op> trees = new ArrayList<>();
        ArrayDeque<Op> stack = new ArrayDeque<>();
        for (Op core : nodes) {
            if (!visited.contains(core))
                trees.add(getJoinTree(core, jg, visited, stack));
        }
        assert !trees.isEmpty();
        Op op = trees.size() == 1 ? trees.get(0) : new CartesianOp(trees);
        op.modifiers().addAll(parent.modifiers());
        StepUtils.exposeFilterVars(op);
        return op;
    }

    private @Nonnull Op getJoinTree(@Nonnull Op core, @Nonnull JoinGraph jg,
                                    @Nonnull IndexedSubset<Op> visited,
                                    @Nonnull ArrayDeque<Op> stack) {
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
