package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface EmptyOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull EmptyOp node);
}
