package br.ufsc.lapesd.riefederator.model.term;

import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface URI extends Res {
    /**
     * Gets the full expanded URI of a resouce
     */
    @Nonnull String getURI();
}
