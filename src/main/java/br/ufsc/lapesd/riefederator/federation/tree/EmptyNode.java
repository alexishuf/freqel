package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.Solution;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class EmptyNode extends PlanNode {
    public EmptyNode(@Nonnull Collection<String> resultVars) {
        this(resultVars, emptySet());
    }

    public EmptyNode(@Nonnull Collection<String> resultVars,
                     @Nonnull Collection<String> inputVars) {
        super(resultVars, false, inputVars, emptyList());
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        HashSet<String> results = new HashSet<>(getResultVars());
        HashSet<String> inputs = new HashSet<>(getInputVars());
        solution.forEach((n, t) -> {
            results.remove(n);
            inputs.remove(n);
        });
        return new EmptyNode(results, inputs);
    }

    @Override
    protected @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        builder.append("EMPTY[");
        Set<String> results = getResultVars(), inputs = getInputVars();
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
        return builder.append("]");
    }
}
