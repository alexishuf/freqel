package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

@Immutable
public class JoinInterface {
    @SuppressWarnings("Immutable")
    private final @Nonnull Set<String> resultVars, inputVars;
    @SuppressWarnings("Immutable")
    private final @Nonnull Set<Triple> matchedTriples;

    public JoinInterface(@Nonnull Op node) {
        Preconditions.checkArgument(!node.getMatchedTriples().isEmpty());
        this.resultVars = node.getResultVars();
        this.inputVars = node.getRequiredInputVars();
        this.matchedTriples = node.getMatchedTriples();
    }

    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    public @Nonnull Set<String> getInputVars() {
        return inputVars;
    }

    public @Nonnull Set<Triple> getMatchedTriples() {
        return matchedTriples;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('[');
        for (String out : resultVars)
            b.append(inputVars.contains(out) ? "->" : "").append("?").append(out).append(", ");
        for (String in : inputVars) {
            if (!resultVars.contains(in))
                b.append("*->?").append(in).append(", ");
        }
        if (!resultVars.isEmpty() || !inputVars.isEmpty())
            b.setLength(b.length()-2);
        b.append("]{");

        assert !matchedTriples.isEmpty();
        for (Triple triple : matchedTriples)
            b.append(triple.toString()).append(", ");
        b.setLength(b.length()-2);
        return b.append('}').toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinInterface)) return false;
        JoinInterface that = (JoinInterface) o;
        return resultVars.equals(that.resultVars) &&
                inputVars.equals(that.inputVars) &&
                matchedTriples.equals(that.matchedTriples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultVars, inputVars, matchedTriples);
    }
}
