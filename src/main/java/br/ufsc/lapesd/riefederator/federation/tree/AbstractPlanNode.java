package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public abstract class AbstractPlanNode implements PlanNode {
    private @Nonnull Set<String> resultVars, inputVars;
    private @Nullable @LazyInit Set<String> strictResultVars = null;
    private boolean projecting;
    private @Nonnull List<PlanNode> children;
    private @Nonnull Cardinality cardinality;
    protected @Nonnull SoftReference<Set<Triple>> matchedTriples
            = new SoftReference<>(null);

    protected AbstractPlanNode(@Nonnull Collection<String> resultVars, boolean projecting,
                               @Nonnull Collection<String> inputVars,
                               @Nonnull List<PlanNode> children,
                               @Nonnull Cardinality cardinality) {
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : ImmutableSet.copyOf(resultVars);
        this.inputVars = inputVars instanceof Set ? (Set<String>)inputVars
                                                  : ImmutableSet.copyOf(inputVars);
        this.projecting = projecting;
        this.children = children;
        this.cardinality = cardinality;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    @Override
    public @Nonnull Set<String> getStrictResultVars() {
        if (inputVars.isEmpty()) return resultVars;
        if (strictResultVars == null)
            strictResultVars = TreeUtils.setMinus(resultVars, inputVars);
        return strictResultVars;
    }

    @Override
    public @Nonnull Set<String> getInputVars() {
        return inputVars;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        Set<Triple> strong = matchedTriples.get();
        if (strong == null) {
            strong = new HashSet<>();
            for (PlanNode child : getChildren())
                strong.addAll(child.getMatchedTriples());
            matchedTriples = new SoftReference<>(strong);
        }
        return strong;
    }

    @Override
    public boolean hasInputs() {
        return !inputVars.isEmpty();
    }

    @Override
    public @Nonnull List<PlanNode> getChildren() {
        return children;
    }

    @Override
    public boolean isProjecting() {
        return projecting;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return cardinality;
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
        Set<String> results = getResultVars(), inputs = getInputVars();
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
