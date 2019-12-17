package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.jena.model.term.JenaLit;
import br.ufsc.lapesd.riefederator.jena.model.term.JenaRes;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDstring;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TripleTest {

    @DataProvider
    public Object[][] equalsData() {
        StdURI stdAlice = new StdURI("http://example.org/Alice");
        JenaRes jenaAlice = fromJena(createResource(stdAlice.getURI()));
        StdURI stdBob = new StdURI("http://example.org/Bob");
        JenaRes jenaBob = fromJena(createResource(stdBob.getURI()));
        StdURI stdKnows = new StdURI(FOAF.knows.getURI());
        JenaRes jenaKnows = fromJena(createResource(stdKnows.getURI()));
        StdURI stdName = new StdURI(FOAF.name.getURI());
        JenaRes jenaName = fromJena(createResource(stdName.getURI()));
        StdBlank stdBlank = new StdBlank();
        JenaRes jenaBlank = fromJena(createResource());
        StdLit alicePT = StdLit.fromUnescaped("alice", "pt");
        StdLit aliceEN = StdLit.fromUnescaped("alice", "en");
        JenaLit aliceENJena = fromJena(createLangLiteral("alice", "en"));
        StdLit stdStr = StdLit.fromUnescaped("alice", new StdURI(XSDstring.getURI()));
        JenaLit jenaStr = fromJena(createTypedLiteral("alice", XSDstring));
        StdVar x1 = new StdVar("x"), x2 = new StdVar("x");
        StdVar y1 = new StdVar("y");


        return asList(
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              new Triple(stdAlice,  stdKnows,  stdBob), true},
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              new Triple(stdAlice,  stdKnows,  jenaBob), true},
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              new Triple(stdAlice,  jenaKnows, stdBob), true},
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              new Triple(jenaAlice, stdKnows,  stdBob), true},
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              null, false},

                new Object[] {new Triple(stdAlice,  stdKnows,  stdBob),
                              new Triple(jenaBob,   stdKnows,  stdAlice), false},
                new Object[] {new Triple(stdAlice,  stdKnows,  stdBlank),
                              new Triple(stdAlice,  stdKnows,  jenaBlank), false},
                new Object[] {new Triple(stdBlank,  stdKnows,  stdBob),
                              new Triple(jenaBlank, stdKnows,  jenaBob), false},

                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  stdName,  aliceEN), true},
                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  stdName,  alicePT), false},
                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  stdName,  aliceENJena), true},
                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  jenaName, aliceENJena), true},

                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  stdName,  stdStr), false},
                new Object[] {new Triple(stdAlice,  stdName,  aliceEN),
                              new Triple(stdAlice,  stdName,  jenaStr), false},
                new Object[] {new Triple(stdAlice,  stdName,  stdStr),
                              new Triple(stdAlice,  stdName,  jenaStr), true},

                new Object[] {new Triple(stdAlice,  stdName,  x1),
                              new Triple(stdAlice,  stdName,  x2), true},
                new Object[] {new Triple(stdAlice,  stdName,  x1),
                              new Triple(stdAlice,  stdName,  y1), false}
        ).toArray(new Object[0][]);
    }

    @DataProvider
    public static Object[][] boundData() {
        StdURI alice = new StdURI("http://example.org/Alice");
        StdURI bob = new StdURI("http://example.org/Bob");
        StdURI knows = new StdURI(FOAF.knows.getURI());
        StdURI name = new StdURI(FOAF.name.getURI());
        StdBlank blank = new StdBlank();
        StdLit lang = StdLit.fromUnescaped("alice", "pt");
        StdLit str = StdLit.fromUnescaped("alice", new StdURI(XSDstring.getURI()));
        StdVar x = new StdVar("x");

        return asList(
                new Object[]{new Triple(alice, knows, bob), true},
                new Object[]{new Triple(alice, name, lang), true},
                new Object[]{new Triple(alice, name, str), true},
                new Object[]{new Triple(blank, knows, bob), true},

                new Object[]{new Triple(alice, knows, x), false},
                new Object[]{new Triple(alice, x, lang), false},
                new Object[]{new Triple(x, name, str), false},
                new Object[]{new Triple(blank, x, bob), false},
                new Object[]{new Triple(alice, x, x), false},
                new Object[]{new Triple(x, x, x), false}
        ).toArray(new Object[0][]);
    }

    @Test(dataProvider = "boundData")
    public void testBound(Triple triple, boolean expected) {
        assertEquals(triple.isBound(), expected);
    }

    @Test
    public void testForEach() {
        StdBlank s = new StdBlank();
        StdURI p = new StdURI(FOAF.name.getURI());
        StdLit o = StdLit.fromEscaped("alice", "pt");
        List<Term> actual = new ArrayList<>(), expected = asList(s, p, o);

        new Triple(s, p, o).forEach(actual::add);
        assertEquals(actual, expected);

        actual.clear();
        new RDFTriple(s, p, o).forEach(actual::add);
        assertEquals(actual, expected);
    }

    @Test
    public void testGet() {
        StdURI alice = new StdURI("http://example.org/Alice");
        StdURI knows = new StdURI(FOAF.knows.getURI());
        StdURI bob = new StdURI("http://example.org/Bob");
        Triple t = new Triple(alice, knows, bob);

        assertEquals(t.get(Triple.Position.SUBJ), alice);
        assertEquals(t.get(Triple.Position.PRED), knows);
        assertEquals(t.get(Triple.Position.OBJ ), bob);
    }

    @Test(dataProvider = "equalsData")
    public void testEquals(@Nonnull Triple left, @Nullable Triple right, boolean expected) {
        if (expected)    assertEquals(left, right);
        else          assertNotEquals(left, right);
    }

    @Test(dataProvider = "equalsData")
    public void testRDFEquals(@Nonnull Triple left, @Nullable Triple right, boolean expected) {
        if (left.isBound() && (right == null || right.isBound())) {
            RDFTriple rLeft = RDFTriple.fromTriple(left);
            RDFTriple rRight = right == null ? null : RDFTriple.fromTriple(right);
            if (expected)    assertEquals(rLeft, rRight);
            else          assertNotEquals(rLeft, rRight);
        }
    }

    @Test(dataProvider = "equalsData")
    public void testRDFEqualsTriple(@Nonnull Triple left, @Nullable Triple right,
                                    boolean expected) {
        if (left.isBound() && (right == null || right.isBound())) {
            RDFTriple rRight = right == null ? null : RDFTriple.fromTriple(right);
            if (expected)    assertEquals(left, rRight);
            else          assertNotEquals(left, rRight);
        }
    }

    @Test
    public void testWith() {
        StdURI alice = new StdURI("http://example.org/Alice");
        StdURI bob = new StdURI("http://example.org/Bob");
        StdURI charlie = new StdURI("http://example.org/Charlie");
        StdURI knows = new StdURI(FOAF.knows.getURI());
        StdURI interest = new StdURI(FOAF.interest.getURI());

        Triple t0 = new Triple(alice, knows, bob);
        Triple t1 = t0.withSubject(charlie);
        Triple t2 = t0.withObject(charlie);
        Triple t3 = t0.withPredicate(interest);

        assertEquals(t0, new Triple(alice, knows, bob));
        assertEquals(t1, new Triple(charlie, knows, bob));
        assertEquals(t2, new Triple(alice, knows, charlie));
        assertEquals(t3, new Triple(alice, interest, bob));
    }
}