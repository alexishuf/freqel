package br.ufsc.lapesd.freqel.federation.execution;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface PlanExecutor {
    @Nonnull Results executePlan(@Nonnull Op plan);
    @Nonnull Results executeNode(@Nonnull Op node);
}
