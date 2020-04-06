package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface QueryNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull QueryNode node);
}
