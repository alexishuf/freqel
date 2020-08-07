package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface MultiQueryOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull UnionOp node);
}
