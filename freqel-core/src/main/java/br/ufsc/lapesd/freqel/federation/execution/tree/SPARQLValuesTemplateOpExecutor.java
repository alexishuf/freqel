package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface SPARQLValuesTemplateOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull SPARQLValuesTemplateOp node);
}
