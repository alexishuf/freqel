package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;

public interface EmptyNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull EmptyNode node);
}
