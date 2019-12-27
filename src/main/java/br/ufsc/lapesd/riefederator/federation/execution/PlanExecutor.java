package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;

public interface PlanExecutor {
    @Nonnull Results execute(@Nonnull PlanNode plan);
}
