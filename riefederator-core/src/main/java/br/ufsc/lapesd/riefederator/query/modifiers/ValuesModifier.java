package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

@Immutable
public class ValuesModifier implements Modifier {
    private final @Nonnull ImmutableSet<String> varNames;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableList<Solution> assignments;

    public ValuesModifier(@Nonnull Collection<String> varNames,
                          @Nonnull Collection<Solution> assignments) {
        this.varNames = ImmutableSet.copyOf(varNames);
        this.assignments = ImmutableList.copyOf(assignments);
    }

    public @Nonnull ImmutableSet<String> getVarNames() {
        return varNames;
    }

    public @Nonnull List<Solution> getAssignments() {
        return assignments;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.VALUES;
    }

    @Override
    public boolean isRequired() {
        return true;
    }
}
