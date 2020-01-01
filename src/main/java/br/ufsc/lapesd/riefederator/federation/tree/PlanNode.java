package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class PlanNode {
    private @Nonnull Set<String> resultVars;
    private boolean projecting;
    private @Nonnull List<PlanNode> children;

    protected PlanNode(@Nonnull Set<String> resultVars, boolean projecting,
                       @Nonnull List<PlanNode> children) {
        this.resultVars = resultVars;
        this.projecting = projecting;
        this.children = children;
    }
    protected PlanNode(@Nonnull Collection<String> resultVars, boolean projecting,
                       @Nonnull List<PlanNode> children) {
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : new HashSet<>(resultVars);
        this.projecting = projecting;
        this.children = children;
    }

    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    public @Nonnull List<PlanNode> getChildren() {
        return children;
    }

    public boolean isProjecting() {
        return projecting;
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
