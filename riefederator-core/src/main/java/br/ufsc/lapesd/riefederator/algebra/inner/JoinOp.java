package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

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
    protected boolean assertAllInvariants(boolean test) {
        if (!test)
            return true;
        if (!JoinInfo.getJoinability(getLeft(), getRight()).isValid())
            return false; // join is invalid
        return super.assertAllInvariants(true);
    }

    @Override
    public void purgeCachesShallow() {
        List<Op> children = getChildren();
        checkState(children.size() == 2, "JoinOp does not have 2 children anymore");
        JoinInfo info = JoinInfo.getJoinability(children.get(0), children.get(1));
        checkArgument(info.isValid(), "Current children of JoinOp cannot be joined!");
        this.joinInfo = info;
        super.purgeCachesShallow();
        assert assertAllInvariants(!joinInfo.equals(info));
    }

    public static class Builder {
        private final @Nonnull JoinOp op;

        public Builder(@Nonnull JoinOp op) {
            this.op = op;
        }

        public @Nonnull Builder add(@Nonnull Modifier modifier) {
            op.modifiers().add(modifier);
            return this;
        }

        public @Nonnull JoinOp build() {
            return op;
        }
    }

    public static @Nonnull Builder builder(@Nonnull Op left, @Nonnull Op right) {
        return new Builder(create(left, right));
    }

    public static @Nonnull JoinOp create(@Nonnull Op left, @Nonnull Op right) {
        return new JoinOp(JoinInfo.getJoinability(left, right));
    }

    public @Nonnull Set<String> getJoinVars() {
        return joinInfo.getJoinVars();
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (reqInputsCache == null) {
            cacheHit = true;
            reqInputsCache = joinInfo.getPendingRequiredInputs();
            if (!modifiers().filters().isEmpty()) {
                reqInputsCache = new HashSet<>(reqInputsCache);
                Set<String> resultVars = getResultVars();
                modifiers().filters().stream().flatMap(f -> f.getVarTermNames().stream())
                           .filter(n -> !resultVars.contains(n)).forEach(reqInputsCache::add);
            }
        }
        return reqInputsCache;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        cacheHit = true;
        return joinInfo.getPendingOptionalInputs();
    }

    public @Nonnull Op getLeft() {
        return getChildren().get(0);
    }

    public @Nonnull Op getRight() {
        return getChildren().get(1);
    }

    @Override
    public @Nonnull Op setChild(int index, @Nonnull Op replacement) {
        Op left  = index == 0 ? replacement : getChildren().get(0);
        Op right = index == 1 ? replacement : getChildren().get(1);
        JoinInfo info = JoinInfo.getJoinability(left, right);
        checkArgument(info.isValid(), "Given replacement node is not joinable");
        Op old = super.setChild(index, replacement);
        this.joinInfo = info;
        return old;
    }

    @Override
    public @Nonnull List<Op> setChildren(@Nonnull List<Op> children) {
        checkArgument(children.size() == 2, "JoinOp must have exactly 2 children");
        JoinInfo info = JoinInfo.getJoinability(children.get(0), children.get(1));
        checkArgument(info.isValid(), "Given children are not joinable!");
        List<Op> old = super.setChildren(children);
        this.joinInfo = info;
        return old;
    }

    @Override
    public void addChild(@Nonnull Op child) {
        throw new UnsupportedOperationException("JoinOp does not allow addChild()");
    }

    @Override
    public @Nonnull Op createWith(@Nonnull List<Op> children, @Nullable Collection<Modifier> mods) {
        checkArgument(children.size() == 2, "A JoinOp requires EXACTLY 2 children");
        JoinOp op = create(children.get(0), children.get(1));
        op.setCardinality(getCardinality());
        if (mods != null) op.modifiers().addAll(mods);
        return op;
    }

    @Override
    public @Nonnull Op createBound(@Nonnull Solution solution) {
        List<Op> list = new ArrayList<>(getChildren().size());
        for (Op child : getChildren())
            list.add(child.createBound(solution));
        assert list.size() == 2;
        JoinInfo info = JoinInfo.getJoinability(list.get(0), list.get(1));
        Op boundOp = info.isValid() ? new JoinOp(info) : new CartesianOp(list);
        boundOp.setCardinality(getCardinality());
        TreeUtils.addBoundModifiers(boundOp.modifiers(), modifiers(), solution);
        return boundOp;
    }

    @Override
    protected @Nonnull String toStringSeparator() {
        return " ⋈ ";
    }

    @Override protected @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append("⋈{").append(String.join(", ", getJoinVars())).append("} ");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinOp)) return false;
        if (!super.equals(o)) return false;
        return joinInfo.equals(((JoinOp) o).joinInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinInfo);
    }
}
