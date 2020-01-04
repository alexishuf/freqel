package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.riefederator.model.RDFUtils.*;
import static org.testng.Assert.assertEquals;

public class RDFUtilsTest {

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
}