package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

@SuppressWarnings("WeakerAccess")
public class RDFUtils {
    private static final Logger logger = LoggerFactory.getLogger(RDFUtils.class);

    private static final @Nonnull String ESCAPES = "tbnrf\"'\\";
    private static final @Nonnull String ESCAPEE = "\t\b\n\r\f\"'\\";

    /**
     * Escapes [1] a lexical form string.
     *
     * [1]: https://www.w3.org/TR/n-triples/#grammar-production-STRING_LITERAL_QUOTE
     *
     * @param alwaysEscape if true, will escape any char in {", \, \n, \r}. If false, will
     *                     not escape if the chars occur in the following sequences:
     *                     {\t \b \n \n \r \f, \", \', \\}
     * @return escaped string or null if input is null
     */
    @Contract(value = "null, _ -> null; !null, _ -> new", pure = true)
    public static String escapeLexicalForm(String lexicalForm, boolean alwaysEscape) {
        if (lexicalForm == null) return null;

        StringBuilder builder = new StringBuilder(lexicalForm.length());
        for (int i = 0; i < lexicalForm.length(); i++) {
            char c = lexicalForm.charAt(i);
            int index = ESCAPEE.indexOf(c);
            if (index >= 0) {
                char next =  '\0';
                if (!alwaysEscape && (i+1) < lexicalForm.length())
                    next = lexicalForm.charAt(i+1);
                if (alwaysEscape || c != '\\' || ESCAPES.indexOf(next) < 0)
                    builder.append('\\');
                builder.append(ESCAPES.charAt(index));
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    @Contract(value = "null -> null", pure = true)
    public static String escapeLexicalForm(String lexicalForm) {
        return escapeLexicalForm(lexicalForm, true);
    }

    @Contract(value = "null -> null; !null -> new", pure = true)
    public static String unescapeLexicalForm(String lexicalForm) {
        if (lexicalForm == null) return null;
        StringBuilder builder = new StringBuilder(lexicalForm.length());
        boolean active = false;
        for (int i = 0; i < lexicalForm.length(); i++) {
            char c = lexicalForm.charAt(i);
            if (active) {
                int index = ESCAPES.indexOf(c);
                if (index >= 0)
                    builder.append(ESCAPEE.charAt(index));
                else
                    logger.warn("Bad Lexical form escape: \\\\{} in {}", c, lexicalForm);
                active = false;
            } else if (!(active = c == '\\')) {
                builder.append(c);
            }
        }
        return builder.toString();
    }
    /**
     * Gives a NTriples [1] representation of the literal
     *
     * [1]: http://www.w3.org/TR/2014/REC-n-triples-20140225/
     */
    public static @Nonnull String toNT(@Nonnull Lit literal) {
        return toTurtle(literal, StdPrefixDict.EMPTY);
    }

    /** See toNT(Lit). */
    public static @Nonnull String toNT(@Nonnull URI uri) {
        return toTurtle(uri, StdPrefixDict.EMPTY);
    }

    public static @Nonnull String toNT(@Nonnull Term term) {
        if (term.isURI()) return toNT(term.asURI());
        else if (term.isLiteral()) return toNT(term.asLiteral());
        else if (term.isBlank()) return "[]";
        throw new IllegalArgumentException("Only URI, Lit and Blank are representable in NTriples");
    }

    /**
     * Gives a Turtle [1] representation of the literal using the prefixes in <code>dict</code>.
     *
     * [1]: http://www.w3.org/TR/2014/REC-turtle-20140225/
     */
    public static @Nonnull String toTurtle(@Nonnull Lit literal, @Nonnull PrefixDict dict) {
        StringBuilder b = new StringBuilder();
        b.append('"').append(escapeLexicalForm(literal.getLexicalForm())).append('"');
        if (literal.getLangTag() != null)
            return b.append('@').append(literal.getLangTag()).toString();
        b.append("^^");
        PrefixDict.Shortened dt = dict.shorten(literal.getDatatype().getURI());
        return b.append(dt.toString("<"+dt.getLongURI()+">")).toString();
    }

    public static @Nonnull String toTurtle(@Nonnull URI uri, @Nonnull PrefixDict dict) {
        return dict.shorten(uri).toString("<"+uri.getURI()+">");
    }
}
