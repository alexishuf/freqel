package br.ufsc.lapesd.riefederator.webapis.requests.parsers;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.NoTermSerializationException;

import javax.annotation.Nonnull;

public interface TermSerializer {
    @Nonnull String
    toString(@Nonnull Term term, @Nonnull String paramName,
             @Nonnull APIRequestExecutor executor) throws NoTermSerializationException;
}
