package br.ufsc.lapesd.freqel.query.annotations;

import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;

public interface MergePolicyAnnotation extends QueryAnnotation {
    boolean canMerge(@Nonnull CQuery a, @Nonnull CQuery b);
}
