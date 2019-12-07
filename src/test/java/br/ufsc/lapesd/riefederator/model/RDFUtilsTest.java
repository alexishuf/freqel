package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static br.ufsc.lapesd.riefederator.model.RDFUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

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

    @DataProvider
    public static Object[][] term2SPARQLData() {
        StdURI xsdInt = new StdURI(XSDDatatype.XSDint.getURI());
        return new Object[][] {
                new Object[] {new StdURI("http://example.org/A"), "<http://example.org/A>"},
                new Object[] {new StdURI(RDFS.Class.getURI()), "rdfs:Class"},
                new Object[] {new StdBlank(), "[]"},
                new Object[] {new StdBlank("bnode03"), "_:bnode03"},
                new Object[] {new StdVar("??asd"), null},
                new Object[] {new StdVar("x"), "?x"},
                new Object[] {StdLit.fromUnescaped("alice", "en"), "\"alice\"@en"},
                new Object[] {StdLit.fromUnescaped("23", xsdInt), "\"23\"^^xsd:int"},
        };
    }

    @Test(dataProvider = "term2SPARQLData")
    public void testTerm2SPARQL(@Nonnull Term term, @Nullable String expected) {
        PrefixDict dict = StdPrefixDict.STANDARD;
        if (expected == null)
            expectThrows(IllegalArgumentException.class, () -> term2SPARQL(term, dict));
        else
            assertEquals(term2SPARQL(term, dict), expected);
    }
}