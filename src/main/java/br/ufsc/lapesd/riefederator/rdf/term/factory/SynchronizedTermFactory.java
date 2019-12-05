package br.ufsc.lapesd.riefederator.rdf.term.factory;

import br.ufsc.lapesd.riefederator.rdf.term.Blank;
import br.ufsc.lapesd.riefederator.rdf.term.Lit;
import br.ufsc.lapesd.riefederator.rdf.term.URI;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;

public class SynchronizedTermFactory implements ThreadSafeTermFactory {
    private final @Nonnull TermFactory delegate;

    public SynchronizedTermFactory(@Nonnull TermFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    @Contract(value = "-> new", pure = true)
    public synchronized @Nonnull Blank createBlank() {
        return delegate.createBlank();
    }

    @Override
    public synchronized @Nonnull URI createURI(@Nonnull String uri) {
        return delegate.createURI(uri);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI, boolean escaped) {
        return delegate.createLit(lexicalForm, datatypeURI, escaped);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull URI datatype, boolean escaped) {
        return delegate.createLit(lexicalForm, datatype, escaped);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI) {
        return delegate.createLit(lexicalForm, datatypeURI);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull URI datatype) {
        return delegate.createLit(lexicalForm, datatype);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm, boolean escaped) {
        return delegate.createLit(lexicalForm, escaped);
    }

    @Override
    public synchronized @Nonnull Lit createLit(@Nonnull String lexicalForm) {
        return delegate.createLit(lexicalForm);
    }

    @Override
    public synchronized @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag, boolean escaped) {
        return delegate.createLangLit(lexicalForm, langTag, escaped);
    }

    @Override
    public synchronized @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag) {
        return delegate.createLangLit(lexicalForm, langTag);
    }

    @Override
    public @Nonnull String toString() {
        return String.format("Synchronized(%s)", delegate.toString());
    }
}
