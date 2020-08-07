package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOp implements Op {
    protected @Nullable Set<String> strictResultVarsCache, publicVarsCache, allInputVarsCache;
    protected @Nonnull String name;
    private @Nonnull Cardinality cardinality = Cardinality.UNSUPPORTED;
    protected @Nonnull Set<OpChangeListener> listeners = new HashSet<>();

    protected final @Nonnull ModifiersSet.Listener modifiersListener = new ModifiersSet.Listener() {
        @Override
        public void added(@Nonnull Modifier modifier) {
            if (modifier instanceof Projection)
                notifyVarsChanged();
        }

        @Override
        public void removed(@Nonnull Modifier modifier) {
            if (modifier instanceof Projection)
                notifyVarsChanged();
        }
    };

    protected AbstractOp() {
        this.name = "n-"+Integer.toHexString(System.identityHashCode(this));
    }

    protected void clearVarsCaches() {
        strictResultVarsCache  = publicVarsCache  = allInputVarsCache = null;
    }

    protected void notifyVarsChanged() {
        clearVarsCaches();
        for (OpChangeListener listener : listeners)
            listener.varsChanged(this);
    }

    @Override
    public @Nonnull String getName() {
        return name;
    }

    @Override
    public void setName(@Nonnull String name) {
        this.name = name;
    }

    protected void assertAllInvariants() {
        assertAllInvariants(true);
    }

    protected boolean assertAllInvariants(boolean test) {
        if (!test || !AbstractOp.class.desiredAssertionStatus())
            return true;
        Projection projection = modifiers().projection();
        assert projection == null || projection.getVarNames().equals(getResultVars());
        assert getPublicVars().containsAll(getResultVars());
        assert getPublicVars().containsAll(getInputVars());
        assert getInputVars().containsAll(getRequiredInputVars());
        assert getInputVars().containsAll(getOptionalInputVars());
        assert getResultVars().containsAll(getStrictResultVars());
        assert getStrictResultVars().stream().noneMatch(getInputVars()::contains);
        clearVarsCaches();
        return true;
    }

    @Override
    public @Nonnull Set<String> getStrictResultVars() {
        if (strictResultVarsCache == null) {
            if (hasInputs())
                strictResultVarsCache = CollectionUtils.setMinus(getResultVars(), getInputVars());
            else
                strictResultVarsCache = getResultVars();
            assert modifiers().projection() == null
                    || requireNonNull(modifiers().projection())
                            .getVarNames().containsAll(strictResultVarsCache);
        }
        return strictResultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getPublicVars() {
        if (publicVarsCache == null)
            publicVarsCache = union(getResultVars(), getInputVars());
        return publicVarsCache;
    }

    @Override
    public @Nonnull Set<String> getInputVars() {
        if (allInputVarsCache == null)
            allInputVarsCache = union(getRequiredInputVars(), getOptionalInputVars());
        return allInputVarsCache;
    }

    @Override
    public boolean hasInputs() {
        return !getOptionalInputVars().isEmpty() || !getRequiredInputVars().isEmpty();
    }

    @Override
    public boolean hasRequiredInputs() {
        return !getRequiredInputVars().isEmpty();
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return Collections.emptySet();
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return Collections.emptySet();
    }

    @Override
    public @Nonnull List<Op> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Op setChild(int index, @NotNull Op replacement) {
        throw new UnsupportedOperationException("setChild() not supported");
    }

    @Override
    public void attachListener(@NotNull OpChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public void detachListener(@NotNull OpChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public @Nonnull Cardinality setCardinality(@Nonnull Cardinality cardinality) {
        Cardinality old = this.cardinality;
        this.cardinality = cardinality;
        return old;
    }

    private @Nonnull String getVarNamesStringContent() {
        Set<String> results = getResultVars(), inputs = getRequiredInputVars();
        if (results.isEmpty() && inputs.isEmpty()) return "";
        StringBuilder builder = new StringBuilder();
        for (String out : results) {
            if (inputs.contains(out))
                builder.append("->");
            builder.append(out).append(", ");
        }
        for (String in : inputs) {
            if (!results.contains(in))
                builder.append("->").append(in).append(", ");
        }
        builder.setLength(builder.length()-2);
        return builder.toString();
    }

    @Override
    public boolean isProjected() {
        return modifiers().projection() != null;
    }

    protected @Nonnull String getPiWithNames() {
        return "π[" + getVarNamesStringContent() + "]";
    }

    protected @Nonnull String getVarNamesString() {
        return (isProjected() ? "π" : "") + "["+getVarNamesStringContent()+"]";
    }

    protected @Nonnull StringBuilder printFilters(@Nonnull StringBuilder builder,
                                                  @Nonnull String indent) {
        modifiers().filters().forEach(f -> builder.append(indent).append(f.getSparqlFilter()).append('\n'));
        return builder;
    }

    @Override
    public @Nonnull String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public @Nonnull String prettyPrint() {
        return prettyPrint(new StringBuilder(), "").toString();
    }

}
