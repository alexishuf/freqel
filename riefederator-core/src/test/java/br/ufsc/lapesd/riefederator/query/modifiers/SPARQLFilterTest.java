package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.core.Var;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
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
            assertTrue(annotation.getTerms().contains(term));
            assertTrue(annotation.getSparqlFilter().matches("^FILTER(.*)$"));
            assertFalse(annotation.getFilterString().matches("^FILTER(.*)$"));
        } else {
            @SuppressWarnings("MismatchedReadAndWriteOfArray") SPARQLFilter[] dummy = {null};
            expectThrows(FilterParsingException.class,
                    () -> dummy[0] = SPARQLFilter.builder(filter).map(var, term).build());
        }
    }

    @Test
    public void testBoundTerms() {
        SPARQLFilter filter = SPARQLFilter.build("?x < 23 && ?x = str(xsd:string)");
        assertEquals(filter.getVars(), singleton("x"));
        assertEquals(filter.getVarTerms(), singleton(x));
        assertEquals(filter.getVarTermNames(), singleton("x"));
        assertEquals(filter.getTerms(), Sets.newHashSet(x, integer(23), xsdString));
    }

    @Test
    public void tesBoundTermsAfterBind() {
        SPARQLFilter filter = SPARQLFilter.build("?x < ?u");
        SPARQLFilter bound = filter.bind(MapSolution.build(u, lit(23)));
        assertEquals(bound.getVars(), singleton("x"));
        assertEquals(bound.getVarTerms(), singleton(x));
        assertEquals(bound.getTerms(), Sets.newHashSet(x, lit(23)));
    }

    @Test
    public void testParseDate() {
        SPARQLFilter filter = SPARQLFilter.build("?u >= \"2019-11-01\"^^xsd:date");
        SPARQLFilterExecutor executor = new SPARQLFilterExecutor();
        assertTrue(executor.evaluate(filter, MapSolution.build(u, date("2019-11-02"))));
        assertFalse(executor.evaluate(filter, MapSolution.build(u, date("2018-11-02"))));
        assertFalse(executor.evaluate(filter, MapSolution.build(u, date("2019-10-12"))));
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

    @DataProvider
    public static @Nonnull Object[][] bindData() {
        Lit i23 = StdLit.fromUnescaped("23", xsdInteger);
        return Stream.of(
                asList(SPARQLFilter.build("?x < ?y"), MapSolution.build(y, i23),
                       SPARQLFilter.build("?x < 23")),
                asList(SPARQLFilter.build("?x < ?y && ?x > 0"), MapSolution.build(y, i23),
                       SPARQLFilter.build("?x < 23 && ?x > 0")),
                asList(SPARQLFilter.build("?x < ?y && ?x >= 0"), MapSolution.build(y, i23),
                       SPARQLFilter.build("?x < 23 && ?x >= 0")),
                asList(SPARQLFilter.build("?x < ?y"), MapSolution.build(z, i23),
                       SPARQLFilter.build("?x < ?y")),
                asList(SPARQLFilter.builder("?x < ?y").map("y", z).build(),
                       MapSolution.build(z, i23),
                       SPARQLFilter.build("?x < 23")),
                asList(SPARQLFilter.builder("?z < ?y").map("z", y)
                                                            .map("y", z).build(),
                       MapSolution.build(z, i23),
                       SPARQLFilter.builder("?z < 23").map("z", y).build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "bindData")
    public void testBind(@Nonnull SPARQLFilter filter, @Nonnull Solution solution,
                         @Nonnull SPARQLFilter bound) {
        assertEquals(filter.bind(solution), bound);
        Set<String> vars = bound.getExpr().getVarsMentioned().stream().map(Var::getVarName)
                                                                      .collect(toSet());
        assertEquals(bound.getVar2Term().keySet(), vars);
        assertEquals(bound.getVarTerms().size(), vars.size());

        Set<String> expectedVarNames;
        expectedVarNames = vars.stream().map(v -> bound.getVar2Term().get(v).asVar().getName())
                                        .collect(toSet());
        assertEquals(bound.getVarTermNames(), expectedVarNames);
    }

    @DataProvider
    public static Object[][] isTrivialData() {
        return Stream.of(
                asList(SPARQLFilter.build("false"), true, false),
                asList(SPARQLFilter.build("true"), true, true),
                asList(SPARQLFilter.build("true || false"), true, true),
                asList(SPARQLFilter.build("true && false "), true, false),
                asList(SPARQLFilter.build("2 > 3 || false"), true, false),
                asList(SPARQLFilter.build("2 < 3 || false"), true, true),
                asList(SPARQLFilter.build("?x > 2 || false"), false, false),
                asList(SPARQLFilter.build("?x > 2 && false"), false, false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "isTrivialData")
    public void testIsTrivial(@Nonnull SPARQLFilter filter,
                              boolean expected, boolean expectedResult) {
        assertEquals(filter.isTrivial(), expected);
        assertEquals(filter.getTrivialResult(), expected ? expectedResult : null);
    }

    @DataProvider
    public static Object[][] withTermVarsUnboundData() {
        return Stream.of(
                asList(SPARQLFilter.build("bound(?x)"), singleton("x"),
                       SPARQLFilter.build("false")),
                asList(SPARQLFilter.build("!bound(?x)"), singleton("x"),
                        SPARQLFilter.build("!false")),
                asList(SPARQLFilter.build("bound(?x) || true"), singleton("x"),
                        SPARQLFilter.build("false || true")),
                asList(SPARQLFilter.build("bound(?x) || 2 > 3"), singleton("x"),
                        SPARQLFilter.build("false || 2 > 3")),
                asList(SPARQLFilter.build("!bound(?x) || true"), singleton("x"),
                        SPARQLFilter.build("!false || true")),
                asList(SPARQLFilter.build("!bound(?x) || 2 > 3"), singleton("x"),
                        SPARQLFilter.build("!false || 2 > 3"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "withTermVarsUnboundData")
    public void testWithTermVarsUnbound(@Nonnull SPARQLFilter filter,
                                       @Nonnull Collection<String> names,
                                       @Nullable SPARQLFilter expected) {
        assertEquals(filter.withVarTermsUnbound(names), expected);
    }
}