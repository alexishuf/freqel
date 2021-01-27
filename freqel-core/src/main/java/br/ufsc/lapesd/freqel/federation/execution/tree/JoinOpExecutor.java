package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface JoinOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull JoinOp node);
}
