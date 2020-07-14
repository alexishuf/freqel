package br.ufsc.lapesd.riefederator.query.annotations;

import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public class NoMergePolicyAnnotation implements MergePolicyAnnotation {
    public static final @Nonnull NoMergePolicyAnnotation INSTANCE = new NoMergePolicyAnnotation();

    @Override
    public boolean canMerge(@Nonnull CQuery a, @Nonnull CQuery b) {
        return false;
    }
}
