package br.ufsc.lapesd.riefederator.rdf.term.factory;

import br.ufsc.lapesd.riefederator.rdf.term.Blank;
import br.ufsc.lapesd.riefederator.rdf.term.Lit;
import br.ufsc.lapesd.riefederator.rdf.term.URI;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class CachedTermFactory implements TermFactory {
    private final  @Nonnull TermFactory delegate;
    private final @Nonnull WeakHashMap<String, URI> uriCache = new WeakHashMap<>();

    private final @Nonnull HashMap<String, WeakHashMap<String, Lit>>
            escapedLitCache = new HashMap<>(128),
            unescapedLitCache = new HashMap<>(128);

    private @Nonnull Lit getCachedLit(boolean escaped, @Nonnull String key,
                                       @Nonnull String lexical,
                                       @Nonnull Supplier<Lit> supplier) {
        HashMap<String, WeakHashMap<String, Lit>> cache;
        cache = escaped ? escapedLitCache : unescapedLitCache;
        return cache.computeIfAbsent(key, k -> new WeakHashMap<>())
                    .computeIfAbsent(lexical, k -> supplier.get());
    }

    public CachedTermFactory(@Nonnull TermFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    @Contract(value = "-> new", pure = true)
    public @Nonnull Blank createBlank() {
        return delegate.createBlank();
    }

    @Override
    public @Nonnull URI createURI(@Nonnull String uri) {
        URI cached = uriCache.get(uri);
        return cached != null ? cached : delegate.createURI(uri);
    }

    @Override
    public @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI,
                                  boolean escaped) {
        return getCachedLit(escaped, datatypeURI, lexicalForm,
                () -> delegate.createLit(lexicalForm, datatypeURI, escaped));
    }

    @Override
    public @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag,
                                      boolean escaped) {
        return getCachedLit(escaped, langTag, lexicalForm,
                () -> delegate.createLangLit(lexicalForm, langTag, escaped));
    }

    @Override
    public @Nonnull String toString() {
        return String.format("Cached(%s)", delegate);
    }
}
