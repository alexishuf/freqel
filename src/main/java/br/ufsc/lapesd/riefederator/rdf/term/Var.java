package br.ufsc.lapesd.riefederator.rdf.term;

import com.google.errorprone.annotations.Immutable;

@Immutable
public interface Var extends Term {
    String getName();
}
