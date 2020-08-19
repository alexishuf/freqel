package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface QueryOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull EndpointQueryOp node);
}
