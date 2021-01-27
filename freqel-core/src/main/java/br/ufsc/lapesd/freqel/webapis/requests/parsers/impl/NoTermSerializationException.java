package br.ufsc.lapesd.freqel.webapis.requests.parsers.impl;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.webapis.requests.impl.APIRequestExecutorException;

import javax.annotation.Nonnull;

public class NoTermSerializationException extends APIRequestExecutorException {
    public @Nonnull Term term;

    public NoTermSerializationException(@Nonnull Term term, @Nonnull String message) {
        super(message);
        this.term = term;
    }

    public NoTermSerializationException(@Nonnull Term term, @Nonnull String message,
                                        @Nonnull Throwable cause) {
        super(message, cause);
        this.term = term;
    }

    public @Nonnull Term getTerm() {
        return term;
    }
}
