package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface PipeOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull PipeOp op);
}
