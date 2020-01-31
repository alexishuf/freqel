package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

@Immutable
public class JoinInterface {
    private static Logger logger = LoggerFactory.getLogger(JoinInterface.class);

    @SuppressWarnings("Immutable")
    private final @Nonnull Set<String> resultVars, inputVars;
    @SuppressWarnings("Immutable")
    private final @Nonnull Set<Triple> matchedTriples;

    public JoinInterface(@Nonnull PlanNode node) {
        Preconditions.checkArgument(!node.getMatchedTriples().isEmpty());
        this.resultVars = node.getResultVars();
        this.inputVars = node.getInputVars();
        this.matchedTriples = node.getMatchedTriples();
        if (!resultVars.containsAll(inputVars)) {
            logger.warn("There are input variables ({}) that are not result variables ({})",
                        inputVars, resultVars);
        }
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
