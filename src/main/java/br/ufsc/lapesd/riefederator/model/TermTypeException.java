package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;

public class TermTypeException extends RuntimeException {
    private final @Nonnull
    Term.Type expected;
    private final @Nonnull Term term;

    public TermTypeException(@Nonnull Term.Type expected, @Nonnull Term term) {
        super(String.format("expected a %s, but got %s", expected, term));
        this.expected = expected;
        this.term = term;
    }

    public @Nonnull Term.Type getExpected() {
        return expected;
    }

    public @Nonnull Term getTerm() {
        return term;
    }
}
