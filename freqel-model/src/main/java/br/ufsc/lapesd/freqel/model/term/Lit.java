package br.ufsc.lapesd.freqel.model.term;

import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public interface Lit extends Term {
    /**
     * Lexical form of the literal. That is, "01" in "01"^^xsd:int.
     */
    @Nonnull String getLexicalForm();

    /**
     * Datatype URI.
     *
     * Note that plain literals have xsd:string as their datatype in RDF 1.1 [1].
     * Language-tagged literals have rdf:langString as datatype.
     *
     * [1]: https://www.w3.org/TR/rdf11-concepts/
     */
    @Nonnull
    URI getDatatype();

    /**
     * Language tag or null (if not a language literal). Returns "en" in Turtle's "@en"
     */
    @Nullable String getLangTag();

    default @Nonnull String toNT() {
        return RDFUtils.toNT(this);
    }
    default  @Nonnull String toTurtle(@Nonnull PrefixDict dict) {
        return RDFUtils.toTurtle(this, dict);
    }

    @Override
    default @Nonnull String toString(@Nonnull PrefixDict dict) {
        return toTurtle(dict);
    }
}
