package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface MultiQueryNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull MultiQueryNode node);
}
