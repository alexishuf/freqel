package br.ufsc.lapesd.riefederator.federation.tree;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class PlanNode {
    private @Nonnull Set<String> resultVars;
    private boolean projecting;

    protected PlanNode(@Nonnull Set<String> resultVars, boolean projecting) {
        this.resultVars = resultVars;
        this.projecting = projecting;
    }
    protected PlanNode(@Nonnull Collection<String> resultVars, boolean projecting) {
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : new HashSet<>(resultVars);
        this.projecting = projecting;
    }

    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    public boolean isProjecting() {
        return projecting;
    }

    protected @Nonnull String getPiWithNames() {
        return "π[" + String.join(",", getResultVars()) + "]";
    }

    @Override
    public @Nonnull String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Contract("_ -> param1") @CanIgnoreReturnValue
    protected abstract @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);
}
