package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

public interface PlanExecutor {
    @Nonnull Results executePlan(@Nonnull Op plan);
    @Nonnull Results executeNode(@Nonnull Op node);
}
