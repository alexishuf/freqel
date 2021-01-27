package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface UnionOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull UnionOp node);
}
