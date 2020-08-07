package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JoinOp extends AbstractInnerOp {
    private @Nonnull JoinInfo joinInfo;

    private static @Nonnull List<Op> toArrayList(@Nonnull Op left, @Nonnull Op right) {
        ArrayList<Op> list = new ArrayList<>(2);
        list.add(left);
        list.add(right);
        return list;
    }

    protected JoinOp(@Nonnull JoinInfo info) {
        super(toArrayList(info.getLeft(), info.getRight()));
        checkArgument(info.isValid(), "Nodes cannot be joined!");
        this.joinInfo = info;
        assertAllInvariants();
    }

    @Override
    protected void clearVarsCaches() {
        List<Op> children = getChildren();
        checkState(children.size() == 2, "JoinOp does not have 2 children anymore");
        JoinInfo info = JoinInfo.getJoinability(children.get(0), children.get(1));
        checkArgument(info.isValid(), "Current children of JoinOp cannot be joined!");
        this.joinInfo = info;
        super.clearVarsCaches();
        assert assertAllInvariants(!joinInfo.equals(info));
    }

    public static @Nonnull JoinOp create(@Nonnull Op left, @Nonnull Op right) {
        return new JoinOp(JoinInfo.getJoinability(left, right));
    }

    public @Nonnull Set<String> getJoinVars() {
        return joinInfo.getJoinVars();
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return joinInfo.getPendingRequiredInputs();
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return joinInfo.getPendingOptionalInputs();
    }

    public @Nonnull Op getLeft() {
        return getChildren().get(0);
    }

    public @Nonnull Op getRight() {
        return getChildren().get(1);
    }

    @Override
    protected @Nonnull Op createWith(@Nonnull List<Op> children) {
        checkArgument(children.size() == 2, "A JoinOp requires EXACTLY 2 children");
        return create(children.get(0), children.get(1));
    }

    @Override
    protected @Nonnull String toStringSeparator() {
        return " ⋈ ";
    }

    @Nonnull @Override protected StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append("⋈{").append(String.join(", ", getJoinVars())).append("} ");
    }
}
