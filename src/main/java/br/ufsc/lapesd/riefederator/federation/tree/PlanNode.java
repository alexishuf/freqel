package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public abstract class PlanNode {
    private @Nonnull Set<String> resultVars, inputVars;
    private boolean projecting;
    private @Nonnull List<PlanNode> children;

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

    protected @Nonnull String getPiWithNames() {
        return "π[" + String.join(",", getResultVars()) + "]";
    }

    /**
     * Return a new tree with the variables in solution bound to the respective {@link Term}s
     *
     * @param solution source of bindings
     * @return new plan tree
     */
    public abstract @Nonnull PlanNode createBound(@Nonnull Solution solution);

    @Override
    public @Nonnull String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Contract("_ -> param1") @CanIgnoreReturnValue
    protected abstract @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);
}
