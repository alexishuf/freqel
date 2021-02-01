package br.ufsc.lapesd.freqel.model.term;

import com.google.errorprone.annotations.Immutable;

@Immutable
public interface Var extends Term {
    String getName();
}
