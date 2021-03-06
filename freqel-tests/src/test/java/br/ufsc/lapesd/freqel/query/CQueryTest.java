package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.query.annotations.MatchAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.annotations.TermAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.PureDescriptive;
import com.google.errorprone.annotations.Immutable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createTolerantQuery;
import static br.ufsc.lapesd.freqel.util.indexed.FullIndexSet.newIndexSet;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class CQueryTest implements TestContext {
    public static final @Nonnull StdURI ageEx = new StdURI("http://example.org/age");
    public static final @Nonnull StdLit AGE_1 =
            StdLit.fromUnescaped("23", new StdURI(XSDDatatype.XSDint.getURI()));

    private static final Atom A1 = new Atom("A1");
    private static final Atom A2 = new Atom("A2");

    /* ~~~ data methods ~~~ */

    @DataProvider
    public static @Nonnull Object[][] containingData() {
        List<Triple> q = asList(
                new Triple(Alice, knows, x), // 0
                new Triple(x, knows, y),     // 1
                new Triple(y, knows, Bob),   // 2
                new Triple(y, age, AGE_1),   // 3
                new Triple(y, p, z),         // 4
                new Triple(p, subPropertyOf, knows),  // 5
                new Triple(z, knows, Alice), // 6
                new Triple(w, knows, w)      // 7
        );
        List<Triple.Position> s   = singletonList(Triple.Position.SUBJ);
        List<Triple.Position> p   = singletonList(Triple.Position.PRED);
        List<Triple.Position> o   = singletonList(Triple.Position.OBJ);
        List<Triple.Position> sp  = asList(Triple.Position.SUBJ, Triple.Position.PRED);
        List<Triple.Position> so  = asList(Triple.Position.SUBJ, Triple.Position.OBJ);
        List<Triple.Position> os  = asList(Triple.Position.OBJ, Triple.Position.SUBJ);
        List<Triple.Position> spo = asList(Triple.Position.SUBJ, Triple.Position.PRED,
                                           Triple.Position.OBJ);
        return new Object[][] {
                new Object[] {q, x, s  , singletonList(q.get(1))},
                new Object[] {q, y, s  , q.subList(2, 5)},
                new Object[] {q, z, so , asList(q.get(4), q.get(6))},
                new Object[] {q, CQueryTest.p, spo, asList(q.get(4), q.get(5))},
                new Object[] {q, CQueryTest.p, sp , asList(q.get(4), q.get(5))},
                new Object[] {q, CQueryTest.p, s  , singletonList(q.get(5))},
                new Object[] {q, CQueryTest.p, p  , singletonList(q.get(4))},
                new Object[] {q, x, so , q.subList(0, 2)},
                new Object[] {q, y, so , q.subList(1, 5)},
                new Object[] {q, x, spo, q.subList(0, 2)},
                new Object[] {q, y, spo, q.subList(1, 5)},
                new Object[] {q, Alice, so , asList(q.get(0), q.get(6))},
                new Object[] {q, Alice, os , asList(q.get(0), q.get(6))},
                new Object[] {q, age, p  , singletonList(q.get(3))},
                new Object[] {q, CQueryTest.p, p  , singletonList(q.get(4))},
                new Object[] {q, w, s  , singletonList(q.get(7))},
                new Object[] {q, w, o  , singletonList(q.get(7))},
                new Object[] {q, w, so , singletonList(q.get(7))},
        };
    }

    @DataProvider
    public static Object[][] streamVarsData() {
        List<Triple> triples = asList(
                new Triple(Alice, knows, Bob),
                new Triple(Bob, knows, x),
                new Triple(x, knows, y),
                new Triple(Alice, p, z));
        return new Object[][] {
                new Object[] {triples.subList(0, 1), emptyList()},
                new Object[] {triples.subList(0, 2), singletonList(x)},
                new Object[] {triples.subList(0, 3), asList(x, y)},
                new Object[] {triples.subList(0, 4), asList(x, y, p, z)},
        };
    }

    @DataProvider
    public static Object[][] streamURIsData() {
        List<Triple> triples = asList(
                new Triple(x, p, y),
                new Triple(x, knows, Alice),
                new Triple(Alice, knows, y),
                new Triple(Alice, knows, Bob));
        return new Object[][] {
                new Object[] {triples.subList(0, 1), emptyList()},
                new Object[] {triples.subList(0, 2), asList(knows, Alice)},
                new Object[] {triples.subList(0, 3), asList(knows, Alice)},
                new Object[] {triples.subList(0, 4), asList(knows, Alice, Bob)},
        };
    }

    /* ~~~ test methods ~~~ */

    @Test
    public void testEmpty() {
        assertEquals(CQuery.EMPTY.size(), 0);
        assertTrue(CQuery.EMPTY.isEmpty());
        //noinspection ReplaceInefficientStreamCount,RedundantOperationOnEmptyContainer
        assertEquals(CQuery.EMPTY.stream().count(), 0L);
    }

    @Test
    @SuppressWarnings({"UseBulkOperation", "ForLoopReplaceableByForEach", "SimplifyStreamApiCallChains"})
    public void testList() {
        Triple t1 = new Triple(Alice, knows, Bob), t2 = new Triple(Bob, knows, Alice);
        CQuery q = CQuery.from(asList(t1, t2));
        assertEquals(q.size(), 2);
        assertFalse(q.isEmpty());
        assertEquals(new ArrayList<>(q), asList(t1, t2));

        List<Triple> copy = new ArrayList<>();
        for (Iterator<Triple> iterator = q.iterator(); iterator.hasNext(); )
            copy.add(iterator.next());
        assertEquals(copy, asList(t1, t2));

        assertEquals(q.stream().collect(toList()), asList(t1, t2));

        assertEquals(q.indexOf(t1), 0);
        assertEquals(q.indexOf(t2), 1);
        assertEquals(q.indexOf(new Triple(Alice, knows, Alice)), -1);

        assertEquals(q.lastIndexOf(t1), 0);
        assertEquals(q.lastIndexOf(t2), 1);
        assertEquals(q.lastIndexOf(new Triple(Alice, knows, Alice)), -1);
    }

    @Test
    public void testEqualsAcceptsList() {
        Triple t1 = new Triple(Alice, knows, Bob), t2 = new Triple(Bob, knows, Alice);
        assertEquals(CQuery.EMPTY, emptyList());
        assertEquals(CQuery.from(t1), singletonList(t1));
        assertEquals(CQuery.from(asList(t1, t2)), asList(t1, t2));

        assertNotEquals(CQuery.from(t1),             emptyList());
        assertNotEquals(CQuery.from(t1),             singletonList(t2));
        assertNotEquals(CQuery.from(t1),             asList(t1, t2));
        assertNotEquals(CQuery.from(asList(t1, t2)), singletonList(t1));
        assertNotEquals(CQuery.from(asList(t1, t2)), asList(t2, t1));
        assertNotEquals(CQuery.EMPTY,                singletonList(t1));
    }

    @Test
    public void testPrefixDictIgnoredOnEquals() {
        CQuery q1 = CQuery.from(new Triple(Alice, knows, x));
        CQuery q2 = CQuery.from(new Triple(Alice, knows, x));
        CQuery qd = q1.withPrefixDict(StdPrefixDict.DEFAULT);

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertEquals(q1, qd);
        assertEquals(q1.hashCode(), qd.hashCode());

        CQuery qd2 = q1.withPrefixDict(StdPrefixDict.STANDARD);
        assertEquals(qd, qd2);
        assertEquals(qd.hashCode(), qd2.hashCode());
    }

    @Test
    public void testEqualsConsidersModifiers() {
        Triple triple = new Triple(x, knows, y);
        CQuery plain1 = CQuery.from(triple);
        CQuery plain2 = CQuery.from(triple);
        CQuery distinct1 = createQuery(triple, Distinct.INSTANCE);
        CQuery distinct2 = createQuery(triple, Distinct.INSTANCE);
        CQuery projected = createQuery(triple, Projection.of("y"));

        assertEquals(plain1, plain2);
        assertEquals(distinct1, distinct2);

        assertEquals(plain1.hashCode(), plain2.hashCode());
        assertEquals(distinct1.hashCode(), distinct2.hashCode());

        assertNotEquals(plain1, distinct1);
        assertNotEquals(plain1, projected);
        assertNotEquals(distinct1, projected);
    }

    @Immutable
    public static class MockAnnotation implements TermAnnotation, TripleAnnotation {
    }

    @Test
    public void testEqualsConsidersTermAnnotations() {
        CQuery qy = createQuery(x, knows, y, new MockAnnotation());
        CQuery qx = createQuery(x, new MockAnnotation(), knows, y);
        MockAnnotation annotation = new MockAnnotation();
        CQuery qk1 = createQuery(x, knows, annotation, y);
        CQuery qk2 = createQuery(x, knows, annotation, y);
        CQuery plain = CQuery.from(new Triple(x, knows, y));

        assertEquals(qy, qy);
        assertEquals(qk1, qk1);
        assertNotEquals(qy, qx);
        assertEquals(qk1, qk2);

        assertNotEquals(qy, plain);
        assertNotEquals(qx, plain);
        assertNotEquals(qk1, plain);

        assertEquals(qx, singletonList(new Triple(x, knows, y)));
    }

    @Test
    public void testEqualsConsidersTripleAnnotations() {
        Triple triple = new Triple(x, knows, y);
        CQuery q1 = createQuery(triple, new MockAnnotation());
        CQuery q2 = createQuery(triple, new MockAnnotation());
        MockAnnotation annotation = new MockAnnotation();
        CQuery qs1 = createQuery(triple, annotation);
        CQuery qs2 = createQuery(triple, annotation);
        CQuery plain = CQuery.from(triple);

        assertEquals(q1, q1);
        assertEquals(qs1, qs1);
        assertNotEquals(q1, q2);
        assertEquals(qs1, qs2);
        assertNotEquals(qs1, plain);

        assertEquals(plain, singletonList(triple));
        assertEquals(q1, singletonList(triple));
    }

    @Test
    public void testGetSelfMatchedSet() {
        CQuery query = CQuery.from(new Triple(x, knows, y), new Triple(x, knows, Bob));
        assertEquals(query.attr().matchedTriples(), newHashSet(
                new Triple(x, knows, y), new Triple(x, knows, Bob)
        ));
    }

    @Test
    public void testGetSelfAndAnnotatedMatchedSet() {
        CQuery query = createQuery(
                x, knows, y,
                x, ageEx, AGE_1, new MatchAnnotation(new Triple(x, age, AGE_1)));
        assertEquals(query.attr().matchedTriples(), newHashSet(
                new Triple(x, knows, y),
                new Triple(x, age, AGE_1)
        ));
    }

    @Test
    public void testIsASK() {
        Triple t1 = new Triple(Alice, knows, Bob), t2 = new Triple(Bob, knows, Alice),
               t3 = new Triple(Bob, knows, x);
        assertTrue(CQuery.from(t1).attr().isAsk());
        assertTrue(CQuery.from(asList(t1, t2)).attr().isAsk());

        assertTrue(CQuery.EMPTY.attr().isAsk());
        assertFalse(CQuery.from(t3).attr().isAsk());
        assertFalse(CQuery.from(asList(t1, t3)).attr().isAsk());
        assertFalse(CQuery.from(asList(t1, t3, t2)).attr().isAsk());
    }

    @Test(dataProvider = "containingData")
    public void testContaining(@Nonnull List<Triple> query, @Nonnull Term term,
                               @Nonnull List<Triple.Position> positions,
                               @Nonnull List<Triple> expected) {
        CQuery cQuery = CQuery.from(query);
        CQuery actual = cQuery.containing(term, positions);
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "containingData")
    public void testParallelContaining(@Nonnull List<Triple> query, @Nonnull Term term,
                               @Nonnull List<Triple.Position> positions,
                               @Nonnull List<Triple> expected) throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        CQuery cQuery = CQuery.from(query);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 32; i++)
                futures.add(exec.submit(
                        () -> assertEquals(cQuery.containing(term, positions), expected)));
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        }
        for (Future<?> f : futures) f.get(); // throws AssertionError as ExecutionException
    }

    @Test
    public void testUnionPreservesAnnotationsAndModifiers() {
        Atom a1 = new Atom("a1"), a2 = new Atom("a2");
        CQuery left = createQuery(
                Alice, knows, x, AtomAnnotation.of(a1),
                x,     age,   u, SPARQLFilterFactory.parseFilter("?u > 23"),
                x,     knows, y);
        CQuery right = createQuery(
                x, AtomAnnotation.of(a1), knows, y,
                y, AtomAnnotation.of(a2), age,   v, SPARQLFilterFactory.parseFilter("?v < 23"),
                y,                        knows, Bob);
        CQuery actual = CQuery.merge(left, right);
        CQuery expected = createQuery(
                Alice, knows, x, AtomAnnotation.of(a1),
                x, age, u, SPARQLFilterFactory.parseFilter("?u > 23"),
                x, knows, y, AtomAnnotation.of(a2),
                y, age, v, SPARQLFilterFactory.parseFilter("?v < 23"),
                y, knows, Bob);
        assertEquals(actual, expected);
        //noinspection SimplifiedTestNGAssertion
        assertTrue(actual.equals(expected));
        assertEquals(actual.getModifiers(), expected.getModifiers());

        Set<ImmutablePair<Term, TermAnnotation>>  leftAnnotations = new HashSet<>();
        Set<ImmutablePair<Term, TermAnnotation>> rightAnnotations = new HashSet<>();
        actual.forEachTermAnnotation((t, a) ->  leftAnnotations.add(ImmutablePair.of(t, a)));
        actual.forEachTermAnnotation((t, a) -> rightAnnotations.add(ImmutablePair.of(t, a)));
        assertEquals(leftAnnotations, rightAnnotations);
    }

    @Test
    public void testContaining() {
        CQuery query = createQuery(
                Alice, knows, x, AtomInputAnnotation.asRequired(A1, "a1").get(),
                                 PureDescriptive.INSTANCE,
                Alice, name, y, AtomInputAnnotation.asOptional(A2, "a2").get(),
                    SPARQLFilterFactory.parseFilter("regex(str(?y), \"^Alice.*\")"),
                x, age, u, SPARQLFilterFactory.parseFilter("?u > 23"),
                Projection.of("x", "y"));
        CQuery sub = query.containing(x, Triple.Position.SUBJ, Triple.Position.OBJ);
        assertEquals(sub.attr().getSet(), newHashSet(
                new Triple(Alice, knows, x),
                new Triple(x, age, u)
        ));

        assertEquals(sub.getModifiers(), newHashSet(
                SPARQLFilterFactory.parseFilter("?u > 23"), Projection.of("x")));
        assertEquals(sub.getTermAnnotations(x),
                     singleton(AtomInputAnnotation.asRequired(A1, "a1").get()));
        assertEquals(sub.getTermAnnotations(y), emptySet());
        assertEquals(sub.getTripleAnnotations(new Triple(Alice, knows, x)),
                     singleton(PureDescriptive.INSTANCE));
    }

    @DataProvider
    public static @Nonnull Object[][] isJoinConnectedData() throws Exception {
        return Stream.of(
                asList(CQuery.EMPTY, true),
                asList(createQuery(Alice, knows, x), true),
                asList(createQuery(Alice, knows, x, x, knows, Bob), true),
                asList(createQuery(Alice, knows, x, Bob, knows, x), true),
                asList(createQuery(Alice, knows, x, Bob, knows, y), false),
                asList(createQuery(Alice, knows, x, Alice, knows, y), false),
                asList(createQuery(Alice, knows, x, Alice, knows, Bob), false),
                asList(createQuery(Alice, knows, x, Alice, knows, y, x, knows, y), true),
                asList(createQuery(Alice, knows, x, Alice, knows, y, x, knows, z, y, knows, z), true),
                asList(LargeRDFBenchSelfTest.loadQuery("S7"), true),
                asList(LargeRDFBenchSelfTest.loadQuery("B2"), true),
                asList(LargeRDFBenchSelfTest.loadQuery("C10"), true),
                asList(LargeRDFBenchSelfTest.loadQuery("B5"), false),
                asList(LargeRDFBenchSelfTest.loadQuery("B6"), false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "isJoinConnectedData")
    public void testIsJoinConnected(@Nonnull Object queryObj, boolean expected) {
        CQuery query = queryObj instanceof CQuery ? (CQuery)queryObj
                                                  : ((QueryOp)queryObj).getQuery();
        assertEquals(query.attr().isJoinConnected(), expected);
    }

    @Test
    public void testOfferVarUniverseSingleton() {
        CQuery q = createQuery(Alice, knows, x);
        q.attr().offerVarNamesUniverse(newIndexSet("x", "y"));
        assertEquals(q.attr().tripleVarNames(),       singleton("x"));
        assertEquals(q.attr().allVarNames(),          singleton("x"));
        assertEquals(q.attr().publicVarNames(),       singleton("x"));
        assertEquals(q.attr().publicTripleVarNames(), singleton("x"));
    }

    @Test
    public void testOfferVarUniverseDistinctSets() {
        CQuery q = createTolerantQuery(x, age, u,
                SPARQLFilterFactory.parseFilter("?u < ?v"), Projection.of("u", "y"));
        q.attr().offerVarNamesUniverse(newIndexSet("u", "v", "x", "y", "z"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("u", "y"));
        assertEquals(q.attr().publicTripleVarNames(), singleton("u"));
    }

    @Test
    public void testOfferUnorderedVarUniverseDistinctSets() {
        CQuery q = createTolerantQuery(x, age, u,
                SPARQLFilterFactory.parseFilter("?u < ?v"), Projection.of("u", "y"));
        q.attr().offerVarNamesUniverse(newIndexSet("z", "y", "x", "v", "u"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("u", "y"));
        assertEquals(q.attr().publicTripleVarNames(), singleton("u"));
    }

    @Test
    public void testOfferVarUniverseThenAddBadProjection() {
        MutableCQuery q = createQuery(Alice, knows, x);
        q.attr().offerVarNamesUniverse(newIndexSet("x", "y"));
        assertEquals(q.attr().allVarNames(),    singleton("x"));
        assertEquals(q.attr().publicVarNames(), newHashSet("x"));

        assertTrue(q.mutateModifiers().add(Projection.of(asList("x", "z"))));

        assertEquals(q.attr().allVarNames(),    singleton("x"));
        assertEquals(q.attr().publicVarNames(), newHashSet("x", "z"));
    }

    @Test
    public void testOfferVarUniverseThenRemoveTriple() {
        MutableCQuery q = createQuery(Alice, knows, x, x, age, u);
        q.attr().offerVarNamesUniverse(newIndexSet("x", "y", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "u"));

        assertEquals(q.remove(1), new Triple(x, age, u));
        assertEquals(q.attr().allVarNames(),          singleton("x"));
        assertEquals(q.attr().tripleVarNames(),       singleton("x"));
        assertEquals(q.attr().publicVarNames(),       singleton("x"));
        assertEquals(q.attr().publicTripleVarNames(), singleton("x"));
    }

    @Test
    public void testOfferVarUniverseMissingFilterVar() {
        MutableCQuery q = createQuery(x, age, u, SPARQLFilterFactory.parseFilter("?u < ?v"));
        q.attr().offerVarNamesUniverse(newIndexSet("x", "y", "u", "z"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "u"));
    }

    @Test
    public void testOfferVarUniverseMissingTripleVar() {
        MutableCQuery q = createQuery(x, age, u, SPARQLFilterFactory.parseFilter("?u < ?v"));
        q.attr().offerVarNamesUniverse(newIndexSet("y", "u", "v", "z"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "u"));
    }

    @Test
    public void testOfferVarUniverseThenRemoveFilter() {
        MutableCQuery q = createQuery(x, age, u, SPARQLFilterFactory.parseFilter("?u < ?v"));
        q.attr().offerVarNamesUniverse(newIndexSet("x", "y", "u", "v", "z"));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("x", "u", "v"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "u"));

        q.mutateModifiers().removeIf(SPARQLFilter.class::isInstance);
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "u"));

        assertTrue(q.mutateModifiers().add(Projection.of("u", "v")));
        assertEquals(q.attr().tripleVarNames(),       newHashSet("x", "u"));
        assertEquals(q.attr().allVarNames(),          newHashSet("x", "u"));
        assertEquals(q.attr().publicVarNames(),       newHashSet("u", "v"));
        assertEquals(q.attr().publicTripleVarNames(), newHashSet("u"));
    }

    private void testTermIndexing(@Nullable IndexSet<Triple> triplesUniverse) {
        MutableCQuery q = createQuery(x, knows, y, x, age, u, y, knows, Bob);
        if (triplesUniverse != null)
            q.attr().offerTriplesUniverse(triplesUniverse);

        assertEquals(q.attr().triplesWithTerm(x),
                newHashSet(new Triple(x, age, u), new Triple(x, knows, y)));
        assertEquals(q.attr().triplesWithTerm(y),
                newHashSet(new Triple(y, knows, Bob), new Triple(x, knows, y)));
        assertEquals(q.attr().triplesWithTermAt(y, Triple.Position.SUBJ),
                singleton(new Triple(y, knows, Bob)));
        assertEquals(q.attr().triplesWithTermAt(Bob, Triple.Position.OBJ),
                singleton(new Triple(y, knows, Bob)));
        assertEquals(q.attr().triplesWithTermAt(age, Triple.Position.PRED),
                singleton(new Triple(x, age, u)));

        assertEquals(q.attr().triplesWithTermAt(age, Triple.Position.SUBJ), emptySet());
        assertEquals(q.attr().triplesWithTermAt(age, Triple.Position.OBJ),  emptySet());
        assertEquals(q.attr().triplesWithTermAt(Bob, Triple.Position.PRED), emptySet());
        assertEquals(q.attr().triplesWithTermAt(x,   Triple.Position.PRED), emptySet());
    }

    @Test
    public void testTermIndexing() {
        testTermIndexing(null);
    }

    @Test
    public void testTermIndexingWithUniverse() {
        IndexSet<Triple> triplesUniverse = newIndexSet(new Triple(y, knows, Bob),
                new Triple(y, age, v),
                new Triple(x, age, u),
                new Triple(x, knows, y));
        testTermIndexing(triplesUniverse);
    }

}