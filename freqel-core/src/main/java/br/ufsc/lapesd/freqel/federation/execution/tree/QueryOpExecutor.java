package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface QueryOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull EndpointQueryOp node);
}
