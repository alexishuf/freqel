package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

import static br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict.EMPTY;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class SPARQLStringTest {
    private final static @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    private final static @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    private final static @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());

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
            expectThrows(IllegalArgumentException.class, () -> SPARQLString.term2SPARQL(term, dict));
        else
            assertEquals(SPARQLString.term2SPARQL(term, dict), expected);
    }

    @Test
    public void testTripleASK() {
        SPARQLString s = new SPARQLString(singleton(new Triple(ALICE, KNOWS, BOB)), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.ASK);
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testConjunctiveASK() {
        SPARQLString s = new SPARQLString(Arrays.asList(
                new Triple(ALICE, KNOWS, BOB), new Triple(ALICE, KNOWS, ALICE)
        ), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.ASK);
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testTripleSELECT() {
        SPARQLString s = new SPARQLString(
                singleton(new Triple(ALICE, KNOWS, new StdVar("who"))), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.SELECT);
        assertEquals(s.getVarNames(), singleton("who"));
    }
}