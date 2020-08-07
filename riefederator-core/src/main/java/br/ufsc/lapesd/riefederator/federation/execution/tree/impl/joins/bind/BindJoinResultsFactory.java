package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface BindJoinResultsFactory {
    /**
     * Create a {@link Results} that executes a bind join.
     *
     * @param smaller Source of {@link Solution}s that bind vars in rightTree
     * @param rightTree Tree that will have variables bound before execution
     * @param joinVars On which {@link Var}s the joined should be executed
     * @param resultVars Which {@link Var}s should be contained in the result.
     * @throws IllegalArgumentException if there are resultVars or joinVars not in the
     *                                  {@link Results#getVarNames()} from smaller nor in the
     *                                  {@link Op#getResultVars()} from rightTree.
     * @return A new {@link Results}
     */
    @Nonnull Results createResults(@Nonnull Results smaller, @Nonnull Op rightTree,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> resultVars);
}
