package br.ufsc.lapesd.freqel.model;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdBlank;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdTermFactory;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.model.RDFUtils.*;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class RDFUtilsTest implements TestContext {

    @DataProvider
    public static Object[][] lexicalData() {
        return new Object[][] {
                {"asd", "asd"},
                {"", ""},
                {" ", " "},
                {"a\tb", "a\\tb"},
                {"\tb", "\\tb"},
                {"a\nb", "a\\nb"},
                {"a\"b", "a\\\"b"},
                {"a\r\nb", "a\\r\\nb"},
                {"a\r\n", "a\\r\\n"},
                {"\"a\"", "\\\"a\\\""},
        };
    }

    @Test(dataProvider = "lexicalData")
    public void testEscapeLexicalForm(String raw, String escaped) {
        assertEquals(escapeLexicalForm(raw), escaped);
    }

    @Test(dataProvider = "lexicalData")
    public void testUnescapeLexicalForm(String raw, String escaped) {
        assertEquals(unescapeLexicalForm(escaped), raw);
    }

    @Test
    public void testURIToTurtle() {
        String actual = toTurtle(new StdURI("http://example.org/asd"), StdPrefixDict.EMPTY);
        assertEquals(actual, "<http://example.org/asd>");
    }

    @Test
    public void testURIToTurtlePrefixed() {
        String actual = toTurtle(new StdURI("http://example.org/asd"), StdPrefixDict.DEFAULT);
        assertEquals(actual, "ex:asd");
    }

    @DataProvider
    public static Object[][] fromNTData() {
        return Stream.of(
                asList(Alice.toNT(), Alice),
                asList("_:b0", new StdBlank("b0", "b0")),
                asList("\"23\"^^<"+xsdInt.getURI()+">", StdLit.fromUnescaped("23", xsdInt)),
                asList("\"-56\"^^<"+xsdInt.getURI()+">", StdLit.fromUnescaped("-56", xsdInt)),
                asList("rdf:type", null), //bad syntax: no prefixes in NT
                asList("\"plain literal\"", StdLit.fromUnescaped("plain literal", xsdString)),
                asList("\"name\"@en", StdLit.fromUnescaped("name", "en")),
                asList("\"name\"@en-US", StdLit.fromUnescaped("name", "en-US")),
                asList("\"\"\"triplequotes\"\"\"@en", StdLit.fromUnescaped("triplequotes", "en")),
                asList("\"\"\"triplequotes\"\"\"@en_US", StdLit.fromUnescaped("triplequotes", "en_US")),
                asList("\"l1\nl2\"@en", StdLit.fromUnescaped("l1\nl2", "en")),
                asList("\"escaped\\\"\"@en", StdLit.fromEscaped("escaped\\\"", "en")),
                asList("\"\\\"escaped\\\"\"@en", StdLit.fromEscaped("\\\"escaped\\\"", "en")),
                asList("\"line1\\nline2\"^^<"+xsdString.getURI()+">",
                       StdLit.fromEscaped("line1\\nline2", xsdString)),
                asList("\"value\"^^<"+xsdString.getURI()+">@en", null), //extra @en
                asList("\"value\"@en^^<"+xsdString.getURI()+">", null), //extra ^^
                /* short forms */
                asList("-5", StdLit.fromUnescaped("-5", xsdInteger)),
                asList("-5.23", StdLit.fromUnescaped("-5.23", xsdDecimal)),
                asList("4.2E9", StdLit.fromUnescaped("4.2E9", xsdDouble)),
                asList("-.4E-2", StdLit.fromUnescaped("-.4E-2", xsdDouble)),
                asList("1e4", StdLit.fromUnescaped("1e4", xsdDouble)),
                asList("true", StdLit.fromUnescaped("true", xsdBoolean)),
                asList("false", StdLit.fromUnescaped("false", xsdBoolean))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "fromNTData")
    public void testFromNT(@Nonnull String string, @Nullable Term term) {
        Term parsed = null;
        try {
            parsed = fromNT(string, new StdTermFactory());
            if (term == null)
                fail("Expected NTParseException");
        } catch (NTParseException e) {
            if (term != null)
                fail("Unexpected NTParseException", e);
        }
        assertEquals(parsed, term);
    }

    @Test
    public void testEmptyFromNT() throws NTParseException {
        assertNull(fromNT("", new StdTermFactory()));
    }

    @Test
    public void testNullFromNT() throws NTParseException {
        assertNull(fromNT(null, new StdTermFactory()));
    }
}