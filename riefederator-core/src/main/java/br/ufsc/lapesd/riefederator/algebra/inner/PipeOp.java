package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PipeOp extends AbstractInnerOp {
    protected PipeOp(@Nonnull Collection<Op> children) {
        super(children);
        assert children.size() == 1;
    }

    public PipeOp(@Nonnull Op child) {
        this(Collections.singletonList(child));
    }

    @Override public @Nonnull List<Op> setChildren(@Nonnull List<Op> children) {
        checkArgument(children.size() == 1, "PieOp MUST have exactly one child");
        return super.setChildren(children);
    }

    @Override public void addChild(@Nonnull Op child) {
        throw new UnsupportedOperationException("A PipeOp cannot addChild()");
    }

    @Override protected @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append("Pipe");
    }

    @Override protected @Nonnull String toStringSeparator() {
        return "|";
    }

    @Override
    public @Nonnull Op createWith(@Nonnull List<Op> children, @Nullable Collection<Modifier> mods) {
        checkArgument(children.size() == 1);
        PipeOp op = new PipeOp(children);
        op.setCardinality(getCardinality());
        if (mods != null)
            op.modifiers().addAll(mods);
        return op;
    }
}
