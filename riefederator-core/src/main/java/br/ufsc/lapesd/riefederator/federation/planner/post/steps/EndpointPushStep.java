package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.DQPushChecker;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLDisjunctiveProfile;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public class EndpointPushStep implements PlannerStep {
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> ignored) {
        if (isTrivial(root))
            return root;
        Visitor.Result result = new Visitor().visit(root);
        DQEndpoint ep = result.ep;
        if (result.role.canOuter() && ep != null)
            return new DQueryOp(ep, root);
        return root;
    }

    private static class Visitor extends DQPushChecker {
        public Visitor() {
            super(SPARQLDisjunctiveProfile.DEFAULT);
        }

        private static final class Result {
            @Nullable DQEndpoint ep;
            @Nonnull Role role;
            boolean profileChecked;

            public Result(@Nullable DQEndpoint ep, @Nonnull Role role, boolean profileChecked) {
                this.ep = ep;
                this.role = role;
                this.profileChecked = profileChecked;
            }

            public static @Nonnull Result none() {
                return new Result(null, Role.NONE, true);
            }
            public static @Nonnull Result checked(@Nonnull DQEndpoint ep, @Nonnull Role role) {
                return new Result(ep, role, true);
            }
            public static @Nonnull Result unchecked(@Nonnull Role role) {
                return new Result(null, role, false);
            }
        }

        public @Nonnull Result visit(@Nonnull Op op) {
            if (op instanceof InnerOp) {
                return visitChildren((InnerOp)op);
            } else {
                if (shared == null) {
                    if (isEmpty(op) || isUnassigned(op)) return Result.unchecked(Role.ANY);
                    DQEndpoint ep = getEndpoint(op);
                    if (ep == null) return Result.none();
                    shared = ep;
                    profile = ep.getDisjunctiveProfile();
                    return Result.checked(ep, allowedRole(op));
                }
                return Result.checked(shared, allowedRole(op));
            }
        }

        public @Nonnull Result visitChildren(@Nonnull InnerOp op) {
            List<Op> children = op.getChildren();
            Result mine = Result.unchecked(Role.ANY);
            Result[] results = new Result[children.size()];
            int size = children.size();
            for (int i = 0; i < size; i++) {
                Result r = visit(children.get(i));
                results[i] = r;
                if (!r.role.canInner()) mine.role = Role.NONE;
                if (r.ep != null) {
                    if (mine.ep == null)      mine.ep = r.ep;
                    else if (mine.ep != r.ep) mine.role = Role.NONE;
                }
            }
            if (mine.ep == null) {
                return mine;
            } else if (mine.role != Role.NONE) {
                profile = mine.ep.getDisjunctiveProfile();
                shared = mine.ep;
                checkUncheckedChildren(children, mine, results);
                if (mine.role != Role.NONE) { //self-check against profile
                    mine.role = allowedRole(op);
                    mine.profileChecked = true;
                }
            }
            if (mine.role == Role.NONE)  //wrap children that can be wrapped
                wrapChildren(op, results, size);
            return mine;
        }

        public void checkUncheckedChildren(List<Op> children, Result mine, Result[] results) {
            assert children.size() == results.length;
            for (int i = 0, size = results.length; i < size; i++) {
                if (!results[i].profileChecked) {
                    results[i].role = allowedTreeRole(children.get(i));
                    if (!results[i].role.canInner())
                        mine.role = Role.NONE;
                    results[i].profileChecked = true;
                }
            }
        }

        private void wrapChildren(InnerOp op, Result[] results, int size) {
            try (TakenChildren taken = op.takeChildren().setNoContentChange()) {
                for (int i = 0; i < size; i++) {
                    DQEndpoint ep = results[i].ep;
                    Op child = taken.get(i);
                    if (ep != null && results[i].role.canOuter() && !isTrivial(child))
                        taken.set(i, new DQueryOp(ep, child));
                }
            }
        }
    }

    private static boolean isTrivial(@Nonnull Op op) {
        if (op instanceof PipeOp)
            return isTrivial(op.getChildren().get(0));
        return !(op instanceof InnerOp);
    }
}
