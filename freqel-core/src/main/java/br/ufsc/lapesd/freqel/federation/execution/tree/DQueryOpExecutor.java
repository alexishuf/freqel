package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface DQueryOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull DQueryOp node);
}
