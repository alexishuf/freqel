package br.ufsc.lapesd.freqel.federation.planner.utils;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;

public interface FilterJoinPlanner {
    @Nonnull Op rewrite(@Nonnull CartesianOp op, @Nonnull RefSet<Op> shared);
    @Nonnull Op rewrite(@Nonnull JoinOp op, @Nonnull RefSet<Op> shared);
}
