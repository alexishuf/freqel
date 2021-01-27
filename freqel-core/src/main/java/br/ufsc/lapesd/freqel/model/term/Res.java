package br.ufsc.lapesd.freqel.model.term;

import com.google.errorprone.annotations.Immutable;

/**
 * A Resource is either an URI or a BLANK node.
 */
@Immutable
public interface Res extends Term {
    default boolean isAnon() {return getType() == Type.BLANK;}
}
