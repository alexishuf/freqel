package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.SPARQLValuesTemplateNode;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface SPARQLValuesTemplateNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull SPARQLValuesTemplateNode node);
}
