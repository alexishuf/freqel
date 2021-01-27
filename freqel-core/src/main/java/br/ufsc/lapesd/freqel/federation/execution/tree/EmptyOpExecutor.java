package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface EmptyOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull EmptyOp node);
}
