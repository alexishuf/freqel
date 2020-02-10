package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.APIRequestExecutorException;

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
