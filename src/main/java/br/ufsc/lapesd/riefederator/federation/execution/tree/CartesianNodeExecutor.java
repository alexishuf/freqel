package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;

public interface CartesianNodeExecutor extends NodeExecutor {
    @Nonnull Results execute(@Nonnull CartesianNode node);
}
