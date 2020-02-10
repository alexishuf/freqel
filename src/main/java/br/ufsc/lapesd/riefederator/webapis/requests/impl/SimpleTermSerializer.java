package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.NoTermSerializationException;
import br.ufsc.lapesd.riefederator.webapis.requests.TermSerializer;

import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static java.lang.String.format;

public class SimpleTermSerializer implements TermSerializer {
    public static final @Nonnull SimpleTermSerializer INSTANCE = new SimpleTermSerializer();

    @Override
    public @Nonnull String toString(@Nonnull Term term, @Nonnull String paramName,
                                    @Nonnull APIRequestExecutor executor)
            throws NoTermSerializationException {

        if (term instanceof Lit) {
            return ((Lit) term).getLexicalForm();
        } else if (term instanceof URI) {
            try {
                return URLEncoder.encode(((URI) term).getURI(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("No support for UTF-8!?");
            }
        } else {
            String msg = format("Cannot bind %s into %s in %s", term, paramName, executor);
            throw new NoTermSerializationException(term, msg);
        }
    }
}
