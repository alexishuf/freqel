package br.ufsc.lapesd.freqel.webapis.requests.parsers;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.NoTermSerializationException;

import javax.annotation.Nonnull;

public interface TermSerializer {
    @Nonnull String
    toString(@Nonnull Term term, @Nonnull String paramName,
             @Nonnull APIRequestExecutor executor) throws NoTermSerializationException;
}
