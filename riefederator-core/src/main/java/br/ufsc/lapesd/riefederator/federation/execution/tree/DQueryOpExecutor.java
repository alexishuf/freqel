package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface DQueryOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull DQueryOp node);
}
