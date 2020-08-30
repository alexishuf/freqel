package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface PipeOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull PipeOp op);
}
