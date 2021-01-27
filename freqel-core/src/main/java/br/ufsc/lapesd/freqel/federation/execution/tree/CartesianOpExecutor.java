package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface CartesianOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull CartesianOp node);
}
