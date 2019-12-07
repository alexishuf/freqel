package br.ufsc.lapesd.riefederator.model.term.factory;

import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;

public interface TermFactory {
    /**
     * Create a new Blank node. Note this will be distinct from any existing blank node
     */
    @Contract(value = "-> new", pure = true)
    @Nonnull
    Blank createBlank();

    /**
     * Create a {@link URI} term from the given long, un-prefixed URI
     */
    @Nonnull URI createURI(@Nonnull String uri);

    /**
     * Creates a typed literal with the given datatype.
     *
     * @param lexicalForm x in "x"^^&lt;y&gt;
     * @param datatypeURI y in "x"^^&lt;y&gt;
     * @param escaped if true, considers lexicalForm contains escapes which will be
     *                expanded to the characters they represent.
     * @return {@link Lit} created or obtained from a cache.
     */
    @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI,
                           boolean escaped);
    default @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull URI datatype,
                                   boolean escaped) {
        return createLit(lexicalForm, datatype.getURI(), escaped);
    }
    default @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI) {
        return createLit(lexicalForm, datatypeURI, false);
    }
    default @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull URI datatype) {
        return createLit(lexicalForm, datatype, false);
    }
    default @Nonnull Lit createLit(@Nonnull String lexicalForm, boolean escaped) {
        return createLit(lexicalForm, "http://www.w3.org/2001/XMLSchema#string",
                         escaped);
    }
    default @Nonnull Lit createLit(@Nonnull String lexicalForm) {
        return createLit(lexicalForm, false);
    }

    /**
     * Creates a language literal (rdf:langString) with the given langTag
     *
     * @param lexicalForm x in turtle's "x"@y.
     * @param langTag y in turtle's "x"@y.
     * @param escaped if true, considers lexicalForm contains escapes which will be
     *                expanded to the characters they represent.
     * @return A {@link Lit} instance, which may be cached.
     */
    @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag,
                               boolean escaped);
    default @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag) {
        return createLangLit(lexicalForm, langTag, false);
    }

    /**
     * Create a {@link Var} instance from its name.
     * @param name var name, the name in ?name
     * @throws UnsupportedOperationException if this {@link TermFactory} cannot create such terms
     */
    @Nonnull Var createVar(@Nonnull String name);
}
