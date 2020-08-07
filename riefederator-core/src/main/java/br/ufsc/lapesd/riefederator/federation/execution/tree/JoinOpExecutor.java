package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface JoinOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull JoinOp node);
}
