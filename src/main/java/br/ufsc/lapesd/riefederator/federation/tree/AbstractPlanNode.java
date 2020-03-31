package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiConsumer;

public abstract class AbstractPlanNode implements PlanNode {
    protected @Nullable Set<String> projection;
    protected @Nullable Set<String> strictResultVarsCache, publicVarsCache, allInputVarsCache;
    private @Nonnull Cardinality cardinality;
    private @Nonnull HashSet<SPARQLFilter> filters = new HashSet<>();

    protected AbstractPlanNode(@Nonnull Cardinality cardinality, @Nullable Set<String> projection) {
        this.projection = projection == null ? null : ImmutableSet.copyOf(projection);
        this.cardinality = cardinality;
    }

    @Override
    public boolean isProjecting() {
        return projection != null;
    }

    protected void assertAllInvariants() {
        if (!getClass().desiredAssertionStatus())
            return;
        if (projection != null) {
            assert isProjecting();
            assert projection.equals(getResultVars());
        }
        assert getPublicVars().containsAll(getResultVars());
        assert getPublicVars().containsAll(getInputVars());
        assert getInputVars().containsAll(getRequiredInputVars());
        assert getInputVars().containsAll(getOptionalInputVars());
        assert getResultVars().containsAll(getStrictResultVars());
        assert getStrictResultVars().stream().noneMatch(getInputVars()::contains);
    }

    @Override
    public @Nonnull Set<String> getStrictResultVars() {
        if (strictResultVarsCache == null) {
            if (hasInputs())
                strictResultVarsCache = TreeUtils.setMinus(getResultVars(), getInputVars());
            else
                strictResultVarsCache = getResultVars();
            assert projection == null || projection.containsAll(strictResultVarsCache);
        }
        return strictResultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getPublicVars() {
        if (publicVarsCache == null)
            publicVarsCache = TreeUtils.union(getResultVars(), getInputVars());
        return publicVarsCache;
    }

    @Override
    public @Nonnull Set<String> getInputVars() {
        if (allInputVarsCache == null)
            allInputVarsCache = TreeUtils.union(getRequiredInputVars(), getOptionalInputVars());
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
    public @Nonnull List<PlanNode> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public boolean addFilter(@Nonnull SPARQLFilter filter) {
        return filters.add(filter);
    }

    @Override
    public boolean removeFilter(@Nonnull SPARQLFilter filter) {
        return filters.remove(filter);
    }

    @Override
    public @Nonnull Set<SPARQLFilter> getFilers() {
        return filters;
    }

    @CanIgnoreReturnValue
    protected boolean addBoundFiltersFrom(@Nonnull Collection<SPARQLFilter> collection,
                                          @Nonnull Solution solution) {
        boolean change = false;
        for (SPARQLFilter filter : collection)
            change |= addFilter(filter.bind(solution));
        return change;
    }

    @Override
    public <T extends TermAnnotation>
    boolean forEachTermAnnotation(@Nonnull Class<T> cls, @Nonnull BiConsumer<Term, T> consumer) {
        boolean has = false;
        for (PlanNode child : getChildren())
            has |= child.forEachTermAnnotation(cls, consumer);
        return has;
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

    protected @Nonnull String getPiWithNames() {
        return "π[" + getVarNamesStringContent() + "]";
    }

    protected @Nonnull String getVarNamesString() {
        return (isProjecting() ? "π" : "") + "[" + getVarNamesStringContent() + "]";
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
