package br.ufsc.lapesd.freqel.description.molecules.tags;

import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface ElementTag {
    @Nonnull String shortDisplayName();
}
