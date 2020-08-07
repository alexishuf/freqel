package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface CartesianOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull CartesianOp node);
}
