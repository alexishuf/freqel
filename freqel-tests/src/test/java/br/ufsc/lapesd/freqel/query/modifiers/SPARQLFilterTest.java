package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
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
                asList("FILTER(?x > 23)", x, true),
                asList("FILTER(?x > 23)", x, true),
                asList("FILTER($x > 23)", x, true),
                asList("FILTER($x > 23)", x, true),
                asList("FILTER(23 > $y)", y, true),
                asList("FILTER(?y = 23)", y, true),
                asList("?x > 23", x, true),
                asList("?x > 23", x, true),
                asList("$x > 23", x, true),
                asList("$x > 23", x, true),
                asList("23 > $y", y, true),
                asList("?y = 23", y, true),

                asList("FILTER()", y, false),
                asList("FILTER()", y, false),
                asList("FILTER", y, false),
                asList("", y, false),
                asList("y", y, false),
                asList("$", y, false),
                asList("?", y, false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String filter, @Nonnull Var singleVar, boolean ok) {
        if (ok) {
            SPARQLFilter annotation = SPARQLFilterFactory.parseFilter(filter);
            assertEquals(annotation.getVars(), singleton(singleVar));
            assertEquals(annotation.getVarNames(), singleton(singleVar.getName()));
            assertTrue(annotation.getTerms().contains(singleVar));
            assertTrue(annotation.getSparqlFilter().matches("^FILTER(.*)$"));
            assertFalse(annotation.getFilterString().matches("^FILTER(.*)$"));
        } else {
            @SuppressWarnings("MismatchedReadAndWriteOfArray") SPARQLFilter[] dummy = {null};
            expectThrows(FilterParsingException.class, () -> dummy[0] = SPARQLFilterFactory.parseFilter(filter));
        }
    }

    @Test
    public void testBoundTerms() {
        SPARQLFilter filter = SPARQLFilterFactory.parseFilter("?x < 23 && ?x = str(xsd:string)");
        assertEquals(filter.getVarNames(), singleton("x"));
        assertEquals(filter.getVars(), singleton(x));
        assertEquals(filter.getVarNames(), singleton("x"));
        assertEquals(filter.getTerms(), Sets.newHashSet(x, integer(23), xsdString));
    }

    @Test
    public void tesBoundTermsAfterBind() {
        SPARQLFilter filter = SPARQLFilterFactory.parseFilter("?x < ?u");
        SPARQLFilter bound = filter.bind(MapSolution.build(u, lit(23)));
        assertEquals(bound.getVarNames(), singleton("x"));
        assertEquals(bound.getVars(), singleton(x));
        assertEquals(bound.getTerms(), Sets.newHashSet(x, lit(23)));

        SPARQLFilter bound2 = filter.bind(t -> t.equals(u) ? lit(23) : t);
        assertEquals(bound2.getVarNames(), singleton("x"));
        assertEquals(bound2.getVars(), singleton(x));
        assertEquals(bound2.getTerms(), Sets.newHashSet(x, lit(23)));
    }

    @Test
    public void testParseDate() {
        SPARQLFilter filter = SPARQLFilterFactory.parseFilter("?u >= \"2019-11-01\"^^xsd:date");
        SPARQLFilterExecutor executor = SPARQLFilterFactory.createExecutor();
        assertTrue(executor.evaluate(filter, MapSolution.build(u, date("2019-11-02"))));
        assertFalse(executor.evaluate(filter, MapSolution.build(u, date("2018-11-02"))));
        assertFalse(executor.evaluate(filter, MapSolution.build(u, date("2019-10-12"))));
    }

    @Test
    public void testFilterQuery() {
        SPARQLFilter ann;
        ann = SPARQLFilterFactory.parseFilter("FILTER($z > 23)");
        CQuery query = createQuery(Alice, knows, y, y, age, z, ann);
        assertEquals(query.getModifiers(), singletonList(ann));
        assertTrue(query.attr().allTerms().contains(integer(23)));
        assertTrue(query.attr().allTerms().contains(StdLit.fromUnescaped("23", xsdInteger)));
    }

    @DataProvider
    public static Object[][] equalsData() {
        return Stream.of(
                asList(
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        true
                ),
                asList(
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        SPARQLFilterFactory.parseFilter("iri(?x) = ?y"),
                        true
                ),
                asList(
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        SPARQLFilterFactory.parseFilter("iri($x)=$y"),
                        true
                ),
                asList(
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?z)"),
                        false
                ),
                asList(
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        SPARQLFilterFactory.parseFilter("FILTER(iri(?x) = ?y)"),
                        true
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
        SPARQLFilter left  = SPARQLFilterFactory.parseFilter(leftString);
        SPARQLFilter right = SPARQLFilterFactory.parseFilter(rightString);
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
        SPARQLFilter left  = SPARQLFilterFactory.parseFilter(leftString);
        SPARQLFilter right = SPARQLFilterFactory.parseFilter(rightString);
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
                asList(SPARQLFilterFactory.parseFilter("?x < ?y"), MapSolution.build(y, i23),
                       SPARQLFilterFactory.parseFilter("?x < 23")),
                asList(SPARQLFilterFactory.parseFilter("?x < ?y && ?x > 0"), MapSolution.build(y, i23),
                       SPARQLFilterFactory.parseFilter("?x < 23 && ?x > 0")),
                asList(SPARQLFilterFactory.parseFilter("?x < ?y && ?x >= 0"), MapSolution.build(y, i23),
                       SPARQLFilterFactory.parseFilter("?x < 23 && ?x >= 0")),
                asList(SPARQLFilterFactory.parseFilter("?x < ?y"), MapSolution.build(z, i23),
                       SPARQLFilterFactory.parseFilter("?x < ?y")),
                asList(SPARQLFilterFactory.parseFilter("?x < ?y"),
                       MapSolution.build(y, i23),
                       SPARQLFilterFactory.parseFilter("?x < 23")),
                asList(SPARQLFilterFactory.parseFilter("?z < ?y"),
                       MapSolution.build(y, i23),
                       SPARQLFilterFactory.parseFilter("?z < 23"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "bindData")
    public void testBind(@Nonnull SPARQLFilter filter, @Nonnull Solution solution,
                         @Nonnull SPARQLFilter bound) {
        assertEquals(filter.bind(solution), bound);
        assertEquals(filter.bind(t -> t.isVar() ? solution.get(t.asVar().getName(), t) : t), bound);
        Set<String> names = bound.getExpr().varsMentioned();
        assertEquals(bound.getVarNames(), names);
        assertEquals(bound.getVars(), names.stream().map(StdVar::new).collect(toSet()));

        Set<String> boundNames = solution.getVarNames().stream()
                .filter(n -> solution.get(n) != null).collect(toSet());
        assertTrue(boundNames.stream().noneMatch(bound.getVarNames()::contains));
    }

    @DataProvider
    public static Object[][] isTrivialData() {
        return Stream.of(
                asList(SPARQLFilterFactory.parseFilter("false"), true, false),
                asList(SPARQLFilterFactory.parseFilter("true"), true, true),
                asList(SPARQLFilterFactory.parseFilter("true || false"), true, true),
                asList(SPARQLFilterFactory.parseFilter("true && false "), true, false),
                asList(SPARQLFilterFactory.parseFilter("2 > 3 || false"), true, false),
                asList(SPARQLFilterFactory.parseFilter("2 < 3 || false"), true, true),
                asList(SPARQLFilterFactory.parseFilter("?x > 2 || false"), false, false),
                asList(SPARQLFilterFactory.parseFilter("?x > 2 && false"), false, false)
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
                asList(SPARQLFilterFactory.parseFilter("bound(?x)"), singleton("x"),
                       SPARQLFilterFactory.parseFilter("false")),
                asList(SPARQLFilterFactory.parseFilter("!bound(?x)"), singleton("x"),
                        SPARQLFilterFactory.parseFilter("!false")),
                asList(SPARQLFilterFactory.parseFilter("bound(?x) || true"), singleton("x"),
                        SPARQLFilterFactory.parseFilter("false || true")),
                asList(SPARQLFilterFactory.parseFilter("bound(?x) || 2 > 3"), singleton("x"),
                        SPARQLFilterFactory.parseFilter("false || 2 > 3")),
                asList(SPARQLFilterFactory.parseFilter("!bound(?x) || true"), singleton("x"),
                        SPARQLFilterFactory.parseFilter("!false || true")),
                asList(SPARQLFilterFactory.parseFilter("!bound(?x) || 2 > 3"), singleton("x"),
                        SPARQLFilterFactory.parseFilter("!false || 2 > 3"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "withTermVarsUnboundData")
    public void testWithTermVarsUnbound(@Nonnull SPARQLFilter filter,
                                       @Nonnull Collection<String> names,
                                       @Nullable SPARQLFilter expected) {
        assertEquals(filter.withVarsEvaluatedAsUnbound(names), expected);
    }
}