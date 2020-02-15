package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.BiConsumer;

public abstract class PlanNode {
    private @Nonnull Set<String> resultVars, inputVars;
    private boolean projecting;
    private @Nonnull List<PlanNode> children;
    protected @Nonnull SoftReference<Set<Triple>> matchedTriples
            = new SoftReference<>(null);

    protected PlanNode(@Nonnull Collection<String> resultVars, boolean projecting,
                       @Nonnull Collection<String> inputVars,
                       @Nonnull List<PlanNode> children) {
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : ImmutableSet.copyOf(resultVars);
        this.inputVars = inputVars instanceof Set ? (Set<String>)inputVars
                                                  : ImmutableSet.copyOf(inputVars);
        this.projecting = projecting;
        this.children = children;
    }

    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    public @Nonnull Set<String> getInputVars() {
        return inputVars;
    }

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

    public boolean hasInputs() {
        return !inputVars.isEmpty();
    }

    public @Nonnull List<PlanNode> getChildren() {
        return children;
    }

    public boolean isProjecting() {
        return projecting;
    }

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

    /**
     * Return a new tree with the variables in solution bound to the respective {@link Term}s
     *
     * @param solution source of bindings
     * @return new plan tree
     */
    public abstract @Nonnull PlanNode createBound(@Nonnull Solution solution);

    /**
     * Creates a new instance replacing the children which are keys in map with
     * the corresponding value.
     *
     * @throws IllegalArgumentException if <code>!getChildren().contains(child)</code>
     * @return a new {@link PlanNode}
     */
    public abstract @Nonnull PlanNode
    replacingChildren(@Nonnull Map<PlanNode, PlanNode> map) throws IllegalArgumentException;

    @Override
    public @Nonnull String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Contract("_ -> param1") @CanIgnoreReturnValue
    protected abstract @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);

    public @Nonnull String prettyPrint() {
        return prettyPrint(new StringBuilder(), "").toString();
    }

    @Contract("_, _ -> param1") @CanIgnoreReturnValue
    protected abstract @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                                          @Nonnull String indent);
}
