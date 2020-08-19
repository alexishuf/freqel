package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

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

    public static @Nonnull ValuesModifier fromResults(@Nonnull Results results) {
        ImmutableList.Builder<Solution> b = ImmutableList.builder();
        results.forEachRemainingThenClose(b::add);
        return new ValuesModifier(results.getVarNames(), b.build());
    }

    public @Nonnull ImmutableSet<String> getVarNames() {
        return varNames;
    }

    public @Nonnull List<Solution> getAssignments() {
        return assignments;
    }

    public @Nonnull Results createResults() {
        return new CollectionResults(assignments, varNames);
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.VALUES;
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder("VALUES (")
                .append(String.join(", ", varNames)).append(") {\n");
        for (Solution solution : getAssignments()) {
            b.append("  (");
            for (String v : varNames) {
                Term term = solution.get(v);
                b.append(term == null ? "UNDEF" : term.toString()).append(' ');
            }
            if (!varNames.isEmpty()) b.setLength(b.length()-1);
            b.append(")\n");
        }
        b.append("}");
        return b.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ValuesModifier)) return false;
        ValuesModifier that = (ValuesModifier) o;
        return getVarNames().equals(that.getVarNames()) &&
                getAssignments().equals(that.getAssignments());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getVarNames(), getAssignments());
    }
}
