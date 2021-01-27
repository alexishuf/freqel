package br.ufsc.lapesd.freqel.federation.cardinality.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Cardinality.Reliability;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class GeneralSelectivityHeuristicTest implements TestContext {
    private final GeneralSelectivityHeuristic heuristic
            = new GeneralSelectivityHeuristic();

    @Test
    public void testSinglePatterns() {
        Cardinality  o = heuristic.estimate(createQuery(Alice, knows, x));
        Cardinality  s = heuristic.estimate(createQuery(x, knows, Alice));
        Cardinality so = heuristic.estimate(createQuery(x, knows, y));

        assertEquals( o.getReliability(), Reliability.GUESS);
        assertEquals( s.getReliability(), Reliability.GUESS);
        assertEquals(so.getReliability(), Reliability.GUESS);
        assertNotEquals( s.getValue(-1), -1);
        assertNotEquals( o.getValue(-1), -1);
        assertNotEquals(so.getValue(-1), -1);

        assertTrue(o.getValue(-1) <  s.getValue(-1));
        assertTrue(s.getValue(-1) < so.getValue(-1));
    }

    @Test
    public void testJoinDisconnectedPenalized() {
        CQuery q = createQuery(Alice, knows, x, y, knows, z);
        Cardinality estimate = heuristic.estimate(q);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        long expected = heuristic.estimate(createQuery(y, knows, x)).getValue(-1);
        assertNotEquals(expected, -1);
        assertNotEquals(estimate, -1);
        assertTrue(estimate.getValue(-1) >= expected);
    }

    @Test
    public void testJoinConnectedNotPenalized() {
        CQuery q = createQuery(Alice, knows, y, x, knows, y);
        Cardinality estimate = heuristic.estimate(q);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);

        long reference = heuristic.estimate(createQuery(x, knows, y)).getValue(-1);
        assertNotEquals(reference, -1);
        assertTrue(estimate.getValue(-1) < reference);
    }

    @Test
    public void testPathHasBonus() {
        CQuery q = createQuery(Alice, knows, x, x, knows, Charlie);
        Cardinality estimate = heuristic.estimate(q);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);

        long reference = heuristic.estimate(createQuery(Alice, knows, x)).getValue(-1)
                      + heuristic.estimate(createQuery(x, knows, Charlie)).getValue(-1);
        assertTrue(estimate.getValue(-1) < reference);
    }

    @Test
    public void testLongPath() {
        CQuery q = createQuery(Alice, knows, x, x, knows, y, y, knows, z, z, knows, Bob);
        Cardinality estimate = heuristic.estimate(q);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);

        long reference = heuristic.estimate(createQuery(x, knows, y)).getValue(-1);
        assertTrue(estimate.getValue(-1) < reference);
        reference = heuristic.estimate(createQuery(Alice, knows, x)).getValue(-1);
        assertTrue(estimate.getValue(-1) > reference);
    }

    @Test
    public void testLongPathWithDisconnected() {
        CQuery q0 = createQuery(Alice, knows, x, x, knows, y, y, knows, z, z, knows, Bob);
        CQuery q1 = createQuery(Alice, knows, x, x, knows, y, y, knows, z, z, knows, Bob,
                               u, knows, v);
        Cardinality estimate = heuristic.estimate(q1);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);

        long reference = heuristic.estimate(q0).getValue(-1);
        assertTrue(estimate.getValue(-1) > reference);
    }

    @Test
    public void testLongerPath() {
        CQuery shortPath = createQuery(Alice, knows, x, x, knows, y,
                                       y, knows, z, z, knows, Bob);
        CQuery longPath = createQuery(Alice, knows, x, x, knows, y,
                                      y, knows, z, z, knows, w, w, knows, Bob);
        Cardinality estimate = heuristic.estimate(longPath);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        long value = estimate.getValue(-1);
        assertNotEquals(value, -1);

        long reference = heuristic.estimate(shortPath).getValue(-1);
        assertTrue(value > reference,  "value="+value+", reference="+reference);

        reference = heuristic.estimate(createQuery(x, knows, y)).getValue(-1);
        assertTrue(value < reference, "value="+value+", reference="+reference);
    }

    @Test
    public void testBonusBadPattern() {
        CQuery q = createQuery(Alice, knows, x, x, knows, y, y, sameAs, z,
                               z, name, lit("Charlie"), z, age, u);
        Cardinality estimate = heuristic.estimate(q);
        assertEquals(estimate.getReliability(), Reliability.GUESS);
        assertNotEquals(estimate.getValue(-1), -1);

        CQuery q2 = createQuery(Alice, knows, x, x, knows, y, y, sameAs, z, z, age, u);
        long reference = heuristic.estimate(q2).getValue(-1);
        assertTrue(estimate.getValue(-1) < reference);
    }

    @Test
    public void testLargeRDFBenchS5() throws SPARQLParseException {
        SPARQLParser parser = SPARQLParser.strict();
        CQuery cheap = parser.parseConjunctive("SELECT * WHERE {\n" +
                "   ?film <http://dbpedia.org/ontology/director>  ?director .\n" +
                "   ?director <http://dbpedia.org/ontology/nationality> <http://dbpedia.org/resource/Italy> .\n" +
                "   ?x <http://www.w3.org/2002/07/owl#sameAs> ?film .\n" +
                "}");
        CQuery expensive = parser.parseConjunctive("SELECT * WHERE {\n" +
                "   ?x <http://www.w3.org/2002/07/owl#sameAs> ?film .\n" +
                "   ?x <http://data.linkedmdb.org/resource/movie/genre> ?genre .\n" +
                "}");
        Cardinality cheapCard = heuristic.estimate(cheap);
        Cardinality expensiveCard = heuristic.estimate(expensive);
        assertEquals(cheapCard.getReliability(), Reliability.GUESS);
        assertEquals(expensiveCard.getReliability(), Reliability.GUESS);
        assertTrue(cheapCard.getValue(-1) < expensiveCard.getValue(-1),
                   "cheap="+cheapCard+", expensive="+expensiveCard);
    }

    @Test
    public void testOpenStarIsBetterThanOpenTriple() throws SPARQLParseException {
        SPARQLParser parser = SPARQLParser.strict();
        CQuery star = parser.parseConjunctive("SELECT * WHERE {\n" +
                "   $drug <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugs> .\n" +
                "   $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> $keggDrug .\n" +
                "   $drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/genericName> $drugBankName .\n" +
                "}\n");
        CQuery triple = parser.parseConjunctive("SELECT * WHERE {\n" +
                "   $chebiDrug <http://bio2rdf.org/ns/bio2rdf#image> $chebiImage .\n" +
                "}\n");
        Cardinality starCard = heuristic.estimate(star);
        Cardinality tripleCard = heuristic.estimate(triple);
        assertEquals(starCard.getReliability(), Reliability.GUESS);
        assertEquals(tripleCard.getReliability(), Reliability.GUESS);

        assertTrue(starCard.getValue(-1) < tripleCard.getValue(Long.MAX_VALUE),
                   "starCard="+starCard+", tripleCard="+tripleCard);
    }

}