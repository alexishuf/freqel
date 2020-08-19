package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.apache.jena.vocabulary.XSD;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromURIResource;

@SuppressWarnings("WeakerAccess")
public class RDFUtils {
    private static final Logger logger = LoggerFactory.getLogger(RDFUtils.class);

    private static final @Nonnull String xsdString = "http://www.w3.org/2001/XMLSchema#string";
    private static final @Nonnull String xsdInteger = "http://www.w3.org/2001/XMLSchema#integer";
    private static final @Nonnull String xsdDouble = "http://www.w3.org/2001/XMLSchema#double";
    private static final @Nonnull String xsdDecimal = "http://www.w3.org/2001/XMLSchema#decimal";
    private static final @Nonnull String xsdBoolean = "http://www.w3.org/2001/XMLSchema#boolean";

    private static final @Nonnull URI xsdIntegerURI = new StdURI(xsdInteger);

    private static final @Nonnull Pattern INTEGER_SHORT_RX =
            Pattern.compile("^[+-]?[0-9]+$");
    private static final @Nonnull Pattern BOOLEAN_SHORT_RX =
            Pattern.compile("^(true|false)$");
    private static final @Nonnull Pattern DECIMAL_SHORT_RX =
            Pattern.compile("^[+-]?[0-9]*\\.[0-9]+$");
    private static final @Nonnull Pattern DOUBLE_SHORT_RX =
            Pattern.compile("^([+-]?[0-9]+\\.[0-9]+|[+-]?\\.[0-9]+|[+-]?[0-9])[eE][+-]?[0-9]+$");

    private static final @Nonnull String ESCAPES = "tbnrf\"'\\";
    private static final @Nonnull String ESCAPEE = "\t\b\n\r\f\"'\\";
    private static final @Nonnull Pattern LIT_RX =
            Pattern.compile("(?:\"|\"\"\")(.*)(?:\"|\"\"\")\\^\\^<([^>]+)>");
    private static final @Nonnull Pattern LANG_RX =
            Pattern.compile("(?:\"|\"\"\")(.*)(?:\"|\"\"\")@(\\w+)");

    private static final @Nonnull Set<URI> INTEGER_DTS;

    static {
        Set<URI> set = new HashSet<>();
        set.add(fromURIResource(XSD.nonPositiveInteger));
        set.add(fromURIResource(XSD.nonNegativeInteger));
        set.add(fromURIResource(XSD.xlong));
        set.add(fromURIResource(XSD.negativeInteger));
        set.add(fromURIResource(XSD.unsignedLong));
        set.add(fromURIResource(XSD.positiveInteger));
        set.add(fromURIResource(XSD.xint));
        set.add(fromURIResource(XSD.xshort));
        set.add(fromURIResource(XSD.xbyte));
        set.add(fromURIResource(XSD.unsignedInt));
        set.add(fromURIResource(XSD.unsignedShort));
        set.add(fromURIResource(XSD.unsignedByte));
        INTEGER_DTS = set;
    }

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

    public static @Nullable Term fromNT(@Nullable String string,
                                        @Nonnull TermFactory termFactory) throws NTParseException {
        if (string == null)
            return null;
        if (string.isEmpty())
            return null;
        if (string.startsWith("_:"))
            return termFactory.createBlank(string.substring(2));
        char first = string.charAt(0), last = string.charAt(string.length() - 1);
        if (first == '<' && last == '>')
            return termFactory.createURI(string.substring(1, string.length()-1));
        else if (first == '"' && last == '"')
            return termFactory.createLit(string.substring(1, string.length()-1), xsdString);

        Matcher matcher = LIT_RX.matcher(string);
        if (!matcher.matches()) {
            matcher = LANG_RX.matcher(string);
            if (!matcher.matches()) {
                try {
                    return fromTurtleShortForm(string, termFactory);
                } catch (NTParseException e) {
                    throw new NTParseException("Bad literal (treated as such since did not " +
                                               "look like _:blank nor <uri>): " + string);
                }
            }
            return termFactory.createLangLit(matcher.group(1), matcher.group(2), true);
        }
        return termFactory.createLit(matcher.group(1), matcher.group(2), true);
    }

    public static @Nullable Term
    fromTurtleShortForm(@Nullable String string,
                        @Nonnull TermFactory termFactory) throws NTParseException {
        if (string == null) return null;
        if (string.isEmpty()) return null;
        assert !string.matches("^\".*\"\\^\\^<.*>$") : "string looks like a type literal";
        assert !string.matches("^\".*\"@\\w+$") : "string looks like a lang literal";
        assert !string.matches("^<.*>$") : "string looks like a URI";

        if (INTEGER_SHORT_RX.matcher(string).matches())
            return termFactory.createLit(string, xsdInteger);
        if (BOOLEAN_SHORT_RX.matcher(string).matches())
            return termFactory.createLit(string.toLowerCase(), xsdBoolean);
        if (DECIMAL_SHORT_RX.matcher(string).matches())
            return termFactory.createLit(string, xsdDecimal);
        if (DOUBLE_SHORT_RX.matcher(string).matches())
            return termFactory.createLit(string, xsdDouble);
        throw new NTParseException(string+" does not match a turtle short form");
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
        String string = uri.getURI();
        string = string.replace("<", "%3C").replace(">", "%3E").replace("`", "%60")
                .replace("{", "%7B").replace("}", "%7D")
                .replace("\"", "%22").replace("'", "%27")
                .replace("|", "%7C").replace(" ", "%20");
        return dict.shorten(string).toString("<"+ string +">");
    }

    public static @Nonnull Solution generalizeLiterals(@Nonnull Solution solution) {
        boolean[] has = {false};
        solution.forEach((n, t) -> {
            if (!has[0] && t.isLiteral() && INTEGER_DTS.contains(t.asLiteral().getDatatype()))
                has[0] = true;
        });
        if (!has[0])
            return solution;
        IndexedSet<String> vars = IndexedSet.fromDistinct(solution.getVarNames());
        Term[] values = new Term[vars.size()];
        int i = 0;
        for (String var : vars) {
            Term t = solution.get(var);
            if (t != null && t.isLiteral() && INTEGER_DTS.contains(t.asLiteral().getDatatype()))
                t = StdLit.fromUnescaped(t.asLiteral().getLexicalForm(), xsdIntegerURI);
            values[i++] = t;
        }
        return new ArraySolution(vars, values);
    }
}
