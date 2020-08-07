package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface SPARQLValuesTemplateOpExecutor extends OpExecutor {
    @Nonnull Results execute(@Nonnull SPARQLValuesTemplateOp node);
}
