package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.OpChangeListener;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public abstract class AbstractInnerOp extends AbstractOp {
    protected  @Nullable Set<String> allVarsCache;
    protected @Nullable Set<String> resultVarsCache, reqInputsCache, optInputsCache;
    private @Nonnull final List<Op> children;
    private @Nullable Set<Triple> matchedTriples;
    private @Nonnull final ModifiersSet modifiers = new ModifiersSet();

    protected final @Nonnull OpChangeListener changeListener = new OpChangeListener() {
        @Override
        public void matchedTriplesChanged(@Nonnull Op op) {
            matchedTriples = null;
            for (OpChangeListener l : listeners)
                l.matchedTriplesChanged(AbstractInnerOp.this);
        }

        @Override
        public void varsChanged(@Nonnull Op op) {
            clearVarsCaches();
            for (OpChangeListener l : listeners)
                l.varsChanged(AbstractInnerOp.this);
        }
    };

    public AbstractInnerOp(@Nonnull Collection<Op> children) {
        this.children = children instanceof List ? (List<Op>)children
                                                 : new ArrayList<>(children);
        modifiers.addListener(modifiersListener);
        for (Op child : this.children)
            child.attachListener(changeListener);
    }

    @Override
    protected void clearVarsCaches() {
        super.clearVarsCaches();
        resultVarsCache = reqInputsCache = optInputsCache = null;
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return modifiers;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        if (allVarsCache == null) {
            allVarsCache = children.stream().flatMap(n -> n.getPublicVars().stream())
                                   .collect(toSet());
        }
        return allVarsCache;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        if (resultVarsCache == null) {
            Projection projection = modifiers().projection();
            if (projection != null)
                resultVarsCache = projection.getVarNames();
            else
                resultVarsCache = CollectionUtils.union(children, Op::getResultVars);
        }
        return resultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (reqInputsCache == null) {
            reqInputsCache = CollectionUtils.union(children, Op::getRequiredInputVars);
            assert reqInputsCache.isEmpty() || hasInputs();
        }
        return reqInputsCache;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        if (optInputsCache == null) {
            Set<String> required = getRequiredInputVars();
            optInputsCache = children.stream().flatMap(n -> n.getOptionalInputVars().stream())
                                      .filter(n -> !required.contains(n))
                                      .collect(toSet());
            assert optInputsCache.isEmpty() || hasInputs();
        }
        return optInputsCache;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        if (matchedTriples == null)
            matchedTriples = CollectionUtils.union(children, Op::getMatchedTriples);
        return matchedTriples;
    }

    @Override
    public @Nonnull List<Op> getChildren() {
        return children;
    }

    @Override
    public @Nonnull Op setChild(int index, @NotNull Op replacement) {
        Preconditions.checkPositionIndex(index, children.size());
        Op old = children.set(index, replacement);
        if (old.equals(replacement)) //compare by ==
            return old; // no effect
        old.detachListener(changeListener);
        changeListener.matchedTriplesChanged(this);
        changeListener.varsChanged(this);
        replacement.attachListener(changeListener);
        return old;
    }

    protected abstract @Nonnull Op createWith(@Nonnull List<Op> children);

    @Override
    public @Nonnull Op createBound(@Nonnull Solution solution) {
        List<Op> list = new ArrayList<>(getChildren().size());
        for (Op child : getChildren())
            list.add(child.createBound(solution));
        Op bound = createWith(list);
        TreeUtils.addBoundModifiers(bound.modifiers(), modifiers(), solution);
        bound.setCardinality(getCardinality());
        return bound;
    }

    protected abstract @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder);

    protected abstract @Nonnull String toStringSeparator();

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        if (isProjected())
            b.append(getPiWithNames()).append('(');
        String sep = " × ";
        for (Op child : getChildren())
            child.toString(b).append(sep);
        b.setLength(b.length()-sep.length());
        if (isProjected())
            b.append(')');
        return b;
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                              @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        prettyPrintNodeType(builder);
        builder.append(isProjected() ? ")" : getVarNamesString())
                .append(' ').append(getCardinality()).append(' ').append(getName());
        if (!modifiers().filters().isEmpty()) {
            builder.append('\n');
            printFilters(builder, indent2);
        }

        if (getChildren().isEmpty())
            return builder;

        for (Op child : getChildren())
            child.prettyPrint(builder, indent2).append('\n');
        builder.setLength(builder.length()-1);
        return builder;
    }
}
