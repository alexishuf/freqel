package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;

public interface JoinNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull JoinNode node);
}
