package br.ufsc.lapesd.freqel.query.annotations;

import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;

public class NoMergePolicyAnnotation implements MergePolicyAnnotation {
    public static final @Nonnull NoMergePolicyAnnotation INSTANCE = new NoMergePolicyAnnotation();

    @Override
    public boolean canMerge(@Nonnull CQuery a, @Nonnull CQuery b) {
        return false;
    }
}
