package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.factory.ThreadSafeTermFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromEscaped;
import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;

@ThreadSafe
public class StdTermFactory implements ThreadSafeTermFactory {
    @Override
    public @Nonnull Blank createBlank() {
        return new StdBlank();
    }

    @Override
    public @Nonnull Blank createBlank(String name) {
        return new StdBlank(name, name);
    }

    @Override
    public @Nonnull URI createURI(@Nonnull String uri) {
        return new StdURI(uri);
    }

    @Override
    public @Nonnull Lit createLit(@Nonnull String lexical, @Nonnull String datatypeURI, boolean escaped) {
        URI uri = createURI(datatypeURI);
        return escaped ? fromEscaped(lexical, uri) : fromUnescaped(lexical, uri);
    }

    @Override
    public @Nonnull Lit createLit(@Nonnull String lexical, @Nonnull URI datatype, boolean escaped) {
        return escaped ? fromEscaped(lexical, datatype) : fromUnescaped(lexical, datatype);
    }

    @Override
    public @Nonnull Lit createLangLit(@Nonnull String lexical, @Nonnull String lang, boolean escaped) {
        return escaped ? fromEscaped(lexical, lang) : fromUnescaped(lexical, lang);
    }

    @Override
    public @Nonnull Var createVar(@Nonnull String name) {
        return new StdVar(name);
    }

    @Override
    public boolean canCreateVar() {
        return true;
    }
}
