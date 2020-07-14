package br.ufsc.lapesd.riefederator.query.annotations;

import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public interface MergePolicyAnnotation extends QueryAnnotation {
    boolean canMerge(@Nonnull CQuery a, @Nonnull CQuery b);
}
