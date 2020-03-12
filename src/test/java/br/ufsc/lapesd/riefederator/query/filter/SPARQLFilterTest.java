package br.ufsc.lapesd.riefederator.query.filter;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.modifiers.FilterParsingException;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class SPARQLFilterTest implements TestContext {

    @DataProvider
    public static Object[][] parseData() {
        return Stream.of(
                asList("FILTER(?x > 23)", "x", x, true),
                asList("FILTER(?x > 23)", "x", y, true),
                asList("FILTER($x > 23)", "x", x, true),
                asList("FILTER($x > 23)", "x", y, true),
                asList("FILTER(23 > $y)", "y", x, true),
                asList("FILTER(?y = 23)", "y", y, true),
                asList("?x > 23", "x", x, true),
                asList("?x > 23", "x", y, true),
                asList("$x > 23", "x", x, true),
                asList("$x > 23", "x", y, true),
                asList("23 > $y", "y", x, true),
                asList("?y = 23", "y", y, true),

                asList("FILTER()", "y", y, false),
                asList("FILTER()", "y", y, false),
                asList("FILTER", "y", y, false),
                asList("", "y", y, false),
                asList("y", "y", y, false),
                asList("$", "y", y, false),
                asList("?", "y", y, false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String filter, @Nonnull String var, @Nonnull Term term,
                          boolean ok) {
        if (ok) {
            SPARQLFilter annotation = SPARQLFilter.builder(filter).map(var, term).build();
            assertEquals(annotation.get(var), term);
            assertNull(annotation.get(var + "~dummy"));
            assertEquals(annotation.getVars(), Collections.singleton(var));
            assertEquals(annotation.getTerms(), Collections.singleton(term));
            assertTrue(annotation.getSparqlFilter().matches("^FILTER(.*)$"));
            assertFalse(annotation.getFilterString().matches("^FILTER(.*)$"));
        } else {
            expectThrows(FilterParsingException.class,
                    () -> SPARQLFilter.builder(filter).map(var, term).build());
        }
    }

    @Test
    public void testParseDate() {
        SPARQLFilter filter = SPARQLFilter.build("?u >= \"2019-11-01\"^^xsd:date");
        assertTrue(filter.evaluate(MapSolution.build(u, date("2019-11-02"))));
        assertFalse(filter.evaluate(MapSolution.build(u, date("2018-11-02"))));
        assertFalse(filter.evaluate(MapSolution.build(u, date("2019-10-12"))));
    }

    @Test
    public void testFilterQuery() {
        SPARQLFilter ann;
        ann = SPARQLFilter.builder("FILTER($input > 23)")
                          .map("input", TestContext.z).build();
        CQuery query = createQuery(Alice, knows, y, y, age, z, ann);
        assertEquals(query.getModifiers(), singletonList(ann));
    }

    @DataProvider
    public static Object[][] equalsData() {
        return Stream.of(
                asList(
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", y).build(),
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", y).build(),
                        true
                ),
                asList(
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                    .map("x", x)
                                    .map("y", y).build(),
                        SPARQLFilter.builder("iri(?x) = ?y")
                                    .map("x", x)
                                    .map("y", y).build(),
                        true
                ),
                asList(
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", y).build(),
                        SPARQLFilter.builder("iri($x)=$y")
                                .map("x", x)
                                .map("y", y).build(),
                        true
                ),
                asList(
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", y).build(),
                        SPARQLFilter.builder("FILTER(iri(?x) = ?z)")
                                .map("x", x)
                                .map("z", z).build(),
                        false
                ),
                asList(
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", y).build(),
                        SPARQLFilter.builder("FILTER(iri(?x) = ?y)")
                                .map("x", x)
                                .map("y", z).build(),
                        false
                )
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "equalsData")
    public void testEquals(SPARQLFilter left, SPARQLFilter right, boolean expected) {
        assertEquals(left.equals(right), expected);
        assertEquals(right.equals(left), expected);
        if (expected)
            assertEquals(left.hashCode(), right.hashCode());
        //noinspection SimplifiedTestNGAssertion,EqualsWithItself
        assertTrue(left.equals(left));
        //noinspection SimplifiedTestNGAssertion,EqualsWithItself
        assertTrue(right.equals(right));
    }


    @DataProvider
    public static Object[][] resultsSubsumedData() {
        return Stream.of(
                // equal filters subsume
                asList("?x > 23", "?x > 23", true),
                asList("?x = 23", "?x = 23", true),
                asList("?x != 23", "?x != 23", true),
                asList("?x >= 23", "?x >= 23", true),
                asList("?x <= 23", "?x <= 23", true),
                // unrelated operators do not subsume
                asList("?x = 23", "?x != 23", false),
                asList("?x = 23", "?x > 23", false),
                // subsumption between > and >=
                asList("?x > 23", "?x >= 23", true),
                asList("?x = 23", "?x >= 23", true),
                asList("?x >= 23", "?x > 23", false),
                // 23 is subsumed by ?y but not the other way
                asList("?x > 23", "?x > ?y", true),
                asList("?x > ?y", "?x > 23", false),
                // changing variable names has no effect...
                asList("?x > 23", "?z > 23", true),
                asList("?x = 23", "?z = 23", true),
                asList("?x != 23", "?z != 23", true),
                asList("?x >= 23", "?z >= 23", true),
                asList("?x <= 23", "?z <= 23", true),
                asList("?x = 23", "?z != 23", false),
                asList("?x = 23", "?z > 23", false),
                asList("?x > 23", "?z >= 23", true),
                asList("?x = 23", "?z >= 23", true),
                asList("?x >= 23", "?z > 23", false),
                asList("?x > 23", "?z > ?y", true),
                asList("?x > ?y", "?z > 23", false),
                // functions must match exactly
                asList("iri(?x) = iri(?y)", "iri(?x) = iri(?y)", true),
                asList("iri(?x) = iri(?y)", "iri(?x) = ?y", false),
                asList("iri(?x) = iri(?y)", "str(?x) = str(?y)", false),
                asList("iri(?x) = ?y", "iri(?x) = iri(?y)", false),
                asList("str(?x) = str(?y)", "iri(?x) = iri(?y)", false),
                // and/or of relational
                asList("?x > 23 && ?x < 32", "?x > 23 && ?x < 32", true),
                asList("?x > 23 && ?x < 32", "?x >= 23 && ?x <= 32", true),
                asList("?x >= 23 && ?x <= 32", "?x > 23 && ?x < 32", false),
                asList("?x < 23 || ?x > 32", "?x < 23 || ?x > 32", true),
                asList("?x < 23 || ?x > 32", "?x <= 23 || ?x >= 32", true),
                asList("?x <= 23 || ?x >= 32", "?x < 23 || ?x > 32", false),
                // read into constants with relational operators
                asList("?x > 23", "?x > 20", true),
                asList("?x > 23", "?x >= 20", true),
                asList("?x > 20", "?x > 23", false),
                asList("?x >= 20", "?x > 23", false),
                asList("?x >= 20", "?x > 19", true),
                asList("?x < 20", "?x < 23", true),
                asList("?x < 23", "?x < 20", false),
                // constants should also be considered wih reverse polish comparison
                asList("23 <= ?x", "20 <= ?x", true),
                asList("23 <= ?x", "20 < ?x", true),
                asList("23 <= ?x", "23 <= ?x", true),
                asList("23 < ?x", "23 < ?x", true),
                asList("23 < ?x", "23 <= ?x", false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "resultsSubsumedData")
    public void testAreResultsSubsumedBy(@Nonnull String leftString,
                                         @Nonnull String rightString, boolean expected) {
        SPARQLFilter left  = SPARQLFilter.build(leftString);
        SPARQLFilter right = SPARQLFilter.build(rightString);
        assertEquals(left.areResultsSubsumedBy(right).getValue(), expected);
    }

    @DataProvider
    public static @Nonnull Object[][] resultsSubsumedMapData() {
        StdLit i20 = StdLit.fromUnescaped("20", xsdInteger);
        StdLit i23 = StdLit.fromUnescaped("23", xsdInteger);
        return Stream.of(
                asList("?x > 23", "?x > ?y", asList(y, i23)),
                asList("?x > 23", "?x > ?y", asList(y, i23, x, x)),
                asList("?x > 23", "?x > 20", asList(i20, i23)),
                asList("?x > 23", "?x > 20", asList(i20, i23, x, x)),
                // same as above, but rely on >= subsuming >
                asList("?x > 23", "?x >= ?y", asList(y, i23)),
                asList("?x > 23", "?x >= ?y", asList(y, i23, x, x)),
                asList("?x > 23", "?x >= 20", asList(i20, i23)),
                asList("?x > 23", "?x >= 20", asList(i20, i23, x, x)),
                // match within function
                asList("?x > str(23)", "?x > str(?y)", asList(x, x, y, i23))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "resultsSubsumedMapData")
    public void testSubsumptionMap(@Nonnull String leftString,
                                   @Nonnull String rightString,
                                   @Nullable List<Term> right2left) {
        SPARQLFilter left  = SPARQLFilter.build(leftString);
        SPARQLFilter right = SPARQLFilter.build(rightString);
        SPARQLFilter.SubsumptionResult result = left.areResultsSubsumedBy(right);
        assertEquals(result.getValue(), right2left != null);

        if (right2left != null) {
            assertEquals(right2left.size() % 2, 0);
            for (Iterator<Term> it = right2left.iterator(); it.hasNext(); ) {
                Term rightTerm = it.next();
                Term leftTerm = it.next();
                assertEquals(result.getOnSubsumed(rightTerm), leftTerm,
                        "expected "+rightTerm+"->"+leftTerm);
            }
        }
    }
}