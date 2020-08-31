package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EndpointPushStep implements PlannerStep {
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> ignored) {
        if (isTrivial(root))
            return root;
        DQEndpoint ep = visit(root);
        return ep == null ? root : new DQueryOp(ep, root);
    }

    private @Nullable DQEndpoint visit(@Nonnull Op root) {
        if (root instanceof InnerOp) {
            DQEndpoint shared = null;
            boolean canWrap = true, canWrapChild = false;
            List<DQEndpoint> eps = new ArrayList<>(root.getChildren().size());
            for (Op child : root.getChildren()) {
                DQEndpoint ep = visit(child);
                eps.add(visit(child));
                canWrapChild |= ep != null && !isTrivial(child);
                if (ep == null) canWrap = false;
                else if (shared == null) shared = ep;
                else if (shared != ep) canWrap = false;
            }
            if (canWrap && shared != null)
                return shared; // parent or plan() will wrap me
            else if (canWrapChild)
                wrapChildren((InnerOp) root, eps);
            return null; // root cannot be wrapped
        } else if (root instanceof EndpointQueryOp) {
            TPEndpoint ep = ((EndpointQueryOp) root).getEndpoint();
            if (ep instanceof DQEndpoint)
                return (DQEndpoint) ep;
        }
        return null;
    }

    private void wrapChildren(@Nonnull InnerOp root, @Nonnull List<DQEndpoint> eps) {
        try (TakenChildren children = root.takeChildren()) {
            for (int i = 0, size = eps.size(); i < size; i++) {
                DQEndpoint ep = eps.get(i);
                if (ep != null)
                    children.set(i, new DQueryOp(ep, children.get(i)));
            }
        }
    }

    private boolean isTrivial(@Nonnull Op op) {
        if (op instanceof PipeOp)
            return isTrivial(op.getChildren().get(0));
        return !(op instanceof InnerOp);
    }
}
