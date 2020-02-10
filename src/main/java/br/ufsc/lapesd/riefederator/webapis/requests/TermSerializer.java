package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;

public interface TermSerializer {
    @Nonnull String
    toString(@Nonnull Term term, @Nonnull String paramName,
             @Nonnull APIRequestExecutor executor) throws NoTermSerializationException;
}
