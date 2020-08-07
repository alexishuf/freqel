package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Set;

public class ProjectingResults extends DelegatingResults implements Results {
    private final @Nonnull ArraySolution.ValueFactory factory;

    public ProjectingResults(@Nonnull Results delegate, @Nonnull Set<String> varNames) {
        super(varNames, delegate);
        this.factory = ArraySolution.forVars(varNames);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        Projection projection = query.getModifiers().projection();
        if (projection != null && !projection.getVarNames().equals(in.getVarNames()))
            return new ProjectingResults(in, projection.getVarNames());
        return in;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull Op op) {
        return op.isProjected() ? new ProjectingResults(in, op.getResultVars()) : in;
    }

    @Override
    public @Nonnull Solution next() {
        return factory.fromSolution(in.next());
    }
}
