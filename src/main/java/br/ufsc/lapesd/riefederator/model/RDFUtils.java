package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

@SuppressWarnings("WeakerAccess")
public class RDFUtils {
    private static final Logger logger = LoggerFactory.getLogger(RDFUtils.class);

    private static final @Nonnull String ESCAPES = "tbnrf\"'\\";
    private static final @Nonnull String ESCAPEE = "\t\b\n\r\f\"'\\";
    private static final @Nonnull Pattern SPARQL_VAR_NAME = Pattern.compile("^[a-zA-Z_0-9\\-]+$");

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

    public static @Nonnull String term2SPARQL(@Nonnull Term t, @Nonnull PrefixDict dict) {
        if (t.isBlank()) {
            String name = t.asBlank().getName();
            return name != null && SPARQL_VAR_NAME.matcher(name).matches() ? "_:"+name : "[]";
        } else if (t.isVar()) {
            String name = t.asVar().getName();
            Preconditions.checkArgument(SPARQL_VAR_NAME.matcher(name).matches(),
                    name+" cannot be used as a SPARQL variable name");
            return "?"+name;
        } else if (t.isLiteral()) {
            return toTurtle(t.asLiteral(), dict);
        } else if (t.isURI()) {
            return toTurtle(t.asURI(), dict);
        }
        throw new IllegalArgumentException("Cannot represent "+t+" in SPARQL");
    }

    public static @Nonnull String term2SPARQL(@Nonnull Term t) {
        return term2SPARQL(t, StdPrefixDict.EMPTY);
    }

    public static @Nonnull String triplePattern2SPARQL(@Nonnull Triple tp) {
        StringBuilder b = new StringBuilder(64);
        if (tp.isBound()) {
            b.append("ASK");
        } else {
            b.append("SELECT");
            int oldLength = b.length();
            tp.forEach(t -> { if (t.isVar()) b.append(" ?").append(t.asVar().getName()); });
            assert b.length() != oldLength;
            b.append(" WHERE");
        }
        b.append(" {\n  ");
        tp.forEach(t -> b.append(term2SPARQL(t)).append(" "));
        b.append(".\n}");
        return b.toString();
    }
}
