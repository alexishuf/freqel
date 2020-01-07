package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;

import static br.ufsc.lapesd.riefederator.query.JoinType.OBJ_SUBJ;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class CQueryTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdURI SUBP = new StdURI(RDFS.subPropertyOf.getURI());
    public static final @Nonnull StdLit AGE_1 =
            StdLit.fromUnescaped("23", new StdURI(XSDDatatype.XSDint.getURI()));
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");
    public static final @Nonnull StdVar W = new StdVar("w");
    public static final @Nonnull StdVar P = new StdVar("p");

    /* ~~~ data methods ~~~ */

    @DataProvider
    public static Object[][] joinClosureData() {
        Triple[] ts = new Triple[]{
                new Triple(ALICE, KNOWS, X    ), // 0
                new Triple(X,     KNOWS, Y    ), // 1
                new Triple(Y,     KNOWS, BOB  ), // 2
                new Triple(Y,     AGE,   AGE_1), // 3
                new Triple(Y,     P,     Z    ), // 4
                new Triple(P,     SUBP,  KNOWS), // 5
                new Triple(Z,     KNOWS, ALICE), // 6
        };
        List<Triple> all = asList(ts);
        return new Object[][] {
                new Object[]{singletonList(ts[0]), ts[0], X, JoinType.ANY,
                        emptyList()},
                new Object[]{singletonList(ts[0]), null, X, JoinType.ANY,
                        singletonList(ts[0])},
                new Object[]{asList(ts[0], ts[1], ts[2]), ts[0], X, JoinType.ANY,
                        asList(ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), null, X, JoinType.ANY,
                        asList(ts[0], ts[1], ts[2])},

                new Object[]{asList(ts[0], ts[1], ts[2]), ts[0], X, JoinType.VARS,
                        asList(ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), null, X, JoinType.VARS,
                        asList(ts[0], ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), ts[0], X, OBJ_SUBJ,
                        asList(ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), null, X, OBJ_SUBJ,
                        asList(ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), ts[1], Y, OBJ_SUBJ,
                        singletonList(ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), null, Y, OBJ_SUBJ,
                        singletonList(ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2]), ts[1], X, OBJ_SUBJ,
                        emptyList()},
                new Object[]{asList(ts[0], ts[1], ts[2]), null, X, OBJ_SUBJ,
                        asList(ts[1], ts[2])},
                new Object[]{asList(ts[0], ts[1], ts[2], ts[6]), ts[1], X, JoinType.SUBJ_OBJ,
                        asList(ts[0], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[2], ts[6]), null, X, JoinType.SUBJ_OBJ,
                        asList(ts[0], ts[6])},

                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], ALICE,
                        JoinType.ANY, asList(ts[0], ts[1], ts[4])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, ALICE,
                        JoinType.ANY, asList(ts[0], ts[1], ts[4], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], ALICE,
                        OBJ_SUBJ, asList(ts[0], ts[1], ts[4])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, ALICE,
                        OBJ_SUBJ, asList(ts[0], ts[1], ts[4], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], ALICE,
                        JoinType.SUBJ_SUBJ, singletonList(ts[0])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, ALICE,
                        JoinType.SUBJ_SUBJ, singletonList(ts[0])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], ALICE,
                        JoinType.SUBJ_OBJ, emptyList()},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, ALICE,
                        JoinType.SUBJ_OBJ, asList(ts[0], ts[1], ts[4], ts[6])},

                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], Z,
                        JoinType.ANY, asList(ts[0], ts[1], ts[4])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, Z,
                        JoinType.ANY, asList(ts[0], ts[1], ts[4], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], Z,
                        JoinType.VARS, asList(ts[0], ts[1], ts[4])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, Z,
                        JoinType.VARS, asList(ts[0], ts[1], ts[4], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], Z,
                        OBJ_SUBJ, emptyList()},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, Z,
                        OBJ_SUBJ, asList(ts[0], ts[1], ts[4], ts[6])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), ts[6], Z,
                        JoinType.SUBJ_OBJ, asList(ts[0], ts[1], ts[4])},
                new Object[]{asList(ts[0], ts[1], ts[4], ts[6]), null, Z,
                        JoinType.SUBJ_OBJ, asList(ts[0], ts[1], ts[4], ts[6])},

                new Object[]{all, ts[0], X, JoinType.ANY,
                        asList(ts[1], ts[2], ts[3], ts[4], ts[5], ts[6])},
                new Object[]{all, null, X, JoinType.ANY, all},
                new Object[]{all, ts[0], X, JoinType.VARS,
                        asList(ts[1], ts[2], ts[3], ts[4], ts[5], ts[6])},
                new Object[]{all, null, X, JoinType.VARS, all},
                new Object[]{all, ts[0], X, OBJ_SUBJ,
                        asList(ts[1], ts[2], ts[3], ts[4], ts[6])},
                new Object[]{all, null, X, OBJ_SUBJ,
                        asList(ts[0], ts[1], ts[2], ts[3], ts[4], ts[6])},
                new Object[]{all, ts[0], X, JoinType.SUBJ_SUBJ, singletonList(ts[1])},
                new Object[]{all, null, X, JoinType.SUBJ_SUBJ, singletonList(ts[1])},
                new Object[]{all, ts[0], X, JoinType.OBJ_OBJ,  emptyList()},
                new Object[]{all, null, X, JoinType.OBJ_OBJ,  singletonList(ts[0])},
        };
    }

    @DataProvider
    public static @Nonnull Object[][] triplesJoinClosureData() {
        Triple[] ts = new Triple[]{
                new Triple(ALICE, KNOWS, X    ), // 0
                new Triple(X,     KNOWS, Y    ), // 1
                new Triple(Y,     KNOWS, BOB  ), // 2
                new Triple(Y,     AGE,   AGE_1), // 3
                new Triple(Y,     P,     Z    ), // 4
                new Triple(P,     SUBP,  KNOWS), // 5
                new Triple(Z,     KNOWS, ALICE), // 6
        };
        List<Triple> all = asList(ts);
        List<Triple> e = emptyList();
        List<Triple> all_0 = all.subList(1, all.size());
        List<Triple> all_0_5 = asList(ts[1], ts[2], ts[3], ts[4], ts[6]);
        List<Triple> all_5_6 = asList(ts[0], ts[1], ts[2], ts[3], ts[4]);
        return new Object[][] {
                /* query, seed, policy, expected (not including seed) */
                new Object[] {singletonList(ts[0]), singletonList(ts[0]), JoinType.ANY, e},
                new Object[] {singletonList(ts[0]), singletonList(ts[0]), JoinType.VARS, e},
                new Object[] {all, singletonList(ts[0]), JoinType.ANY, all_0},
                new Object[] {all, singletonList(ts[0]), OBJ_SUBJ, all_0_5},
                new Object[] {all, singletonList(ts[6]), OBJ_SUBJ, all_5_6},
                new Object[] {all, asList(ts[6], ts[0]), OBJ_SUBJ, all.subList(1, 5)},
                new Object[] {all, asList(ts[6], ts[5], ts[0]), OBJ_SUBJ, all.subList(1, 5)},
        };
    }

    @DataProvider
    public static @Nonnull Object[][] containingData() {
        List<Triple> q = asList(
                new Triple(ALICE, KNOWS, X), // 0
                new Triple(X, KNOWS, Y),     // 1
                new Triple(Y, KNOWS, BOB),   // 2
                new Triple(Y, AGE, AGE_1),   // 3
                new Triple(Y, P, Z),         // 4
                new Triple(P, SUBP, KNOWS),  // 5
                new Triple(Z, KNOWS, ALICE), // 6
                new Triple(W, KNOWS, W)      // 7
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
                new Object[] {q, X    , s  , singletonList(q.get(1))},
                new Object[] {q, Y    , s  , q.subList(2, 5)},
                new Object[] {q, Z    , so , asList(q.get(4), q.get(6))},
                new Object[] {q, P    , spo, asList(q.get(4), q.get(5))},
                new Object[] {q, P    , sp , asList(q.get(4), q.get(5))},
                new Object[] {q, P    , s  , singletonList(q.get(5))},
                new Object[] {q, P    , p  , singletonList(q.get(4))},
                new Object[] {q, X    , so , q.subList(0, 2)},
                new Object[] {q, Y    , so , q.subList(1, 5)},
                new Object[] {q, X    , spo, q.subList(0, 2)},
                new Object[] {q, Y    , spo, q.subList(1, 5)},
                new Object[] {q, ALICE, so , asList(q.get(0), q.get(6))},
                new Object[] {q, ALICE, os , asList(q.get(0), q.get(6))},
                new Object[] {q, AGE  , p  , singletonList(q.get(3))},
                new Object[] {q, P    , p  , singletonList(q.get(4))},
                new Object[] {q, W    , s  , singletonList(q.get(7))},
                new Object[] {q, W    , o  , singletonList(q.get(7))},
                new Object[] {q, W    , so , singletonList(q.get(7))},
        };
    }

    @DataProvider
    public static Object[][] streamVarsData() {
        List<Triple> triples = asList(
                new Triple(ALICE, KNOWS, BOB),
                new Triple(BOB, KNOWS, X),
                new Triple(X, KNOWS, Y),
                new Triple(ALICE, P, Z));
        return new Object[][] {
                new Object[] {triples.subList(0, 1), emptyList()},
                new Object[] {triples.subList(0, 2), singletonList(X)},
                new Object[] {triples.subList(0, 3), asList(X, Y)},
                new Object[] {triples.subList(0, 4), asList(X, Y, P, Z)},
        };
    }

    @DataProvider
    public static Object[][] streamURIsData() {
        List<Triple> triples = asList(
                new Triple(X, P, Y),
                new Triple(X, KNOWS, ALICE),
                new Triple(ALICE, KNOWS, Y),
                new Triple(ALICE, KNOWS, BOB));
        return new Object[][] {
                new Object[] {triples.subList(0, 1), emptyList()},
                new Object[] {triples.subList(0, 2), asList(KNOWS, ALICE)},
                new Object[] {triples.subList(0, 3), asList(KNOWS, ALICE)},
                new Object[] {triples.subList(0, 4), asList(KNOWS, ALICE, BOB)},
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
    @SuppressWarnings("DoNotCall")
    public void testImmutable() {
        CQuery q = CQuery.from(new Triple(ALICE, KNOWS, BOB));
        expectThrows(UnsupportedOperationException.class, q::clear);
        Triple triple = new Triple(BOB, KNOWS, ALICE);
        expectThrows(UnsupportedOperationException.class, () -> q.add(triple));
        expectThrows(UnsupportedOperationException.class, () -> q.remove(triple));
        expectThrows(UnsupportedOperationException.class, () -> q.remove(0));
    }

    @Test
    @SuppressWarnings({"UseBulkOperation", "ForLoopReplaceableByForEach", "SimplifyStreamApiCallChains"})
    public void testList() {
        Triple t1 = new Triple(ALICE, KNOWS, BOB), t2 = new Triple(BOB, KNOWS, ALICE);
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
        assertEquals(q.indexOf(new Triple(ALICE, KNOWS, ALICE)), -1);

        assertEquals(q.lastIndexOf(t1), 0);
        assertEquals(q.lastIndexOf(t2), 1);
        assertEquals(q.lastIndexOf(new Triple(ALICE, KNOWS, ALICE)), -1);
    }

    @Test
    public void testEqualsAcceptsList() {
        Triple t1 = new Triple(ALICE, KNOWS, BOB), t2 = new Triple(BOB, KNOWS, ALICE);
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
        CQuery q1 = CQuery.from(new Triple(ALICE, KNOWS, X));
        CQuery q2 = CQuery.from(new Triple(ALICE, KNOWS, X));
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
        Triple triple = new Triple(X, KNOWS, Y);
        CQuery plain1 = CQuery.from(triple);
        CQuery plain2 = CQuery.from(triple);
        CQuery distinct1 = CQuery.with(triple).distinct().build();
        CQuery distinct2 = CQuery.with(triple).distinct().build();
        CQuery projected = CQuery.with(triple).project("y").build();

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
        CQuery qy = CQuery.with(new Triple(X, KNOWS, Y)).annotate(Y, new MockAnnotation()).build();
        CQuery qx = CQuery.with(new Triple(X, KNOWS, Y)).annotate(X, new MockAnnotation()).build();
        MockAnnotation annotation = new MockAnnotation();
        CQuery qk1 = CQuery.with(new Triple(X, KNOWS, Y)).annotate(KNOWS, annotation).build();
        CQuery qk2 = CQuery.with(new Triple(X, KNOWS, Y)).annotate(KNOWS, annotation).build();
        CQuery plain = CQuery.from(new Triple(X, KNOWS, Y));

        assertEquals(qy, qy);
        assertEquals(qk1, qk1);
        assertNotEquals(qy, qx);
        assertEquals(qk1, qk2);

        assertNotEquals(qy, plain);
        assertNotEquals(qx, plain);
        assertNotEquals(qk1, plain);

        assertEquals(qx, singletonList(new Triple(X, KNOWS, Y)));
    }

    @Test
    public void testEqualsConsidersTripleAnnotations() {
        Triple triple = new Triple(X, KNOWS, Y);
        CQuery q1 = CQuery.with(triple).annotate(triple, new MockAnnotation()).build();
        CQuery q2 = CQuery.with(triple).annotate(triple, new MockAnnotation()).build();
        MockAnnotation annotation = new MockAnnotation();
        CQuery qs1 = CQuery.with(triple).annotate(triple, annotation).build();
        CQuery qs2 = CQuery.with(triple).annotate(triple, annotation).build();
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
    public void testIsASK() {
        Triple t1 = new Triple(ALICE, KNOWS, BOB), t2 = new Triple(BOB, KNOWS, ALICE),
               t3 = new Triple(BOB, KNOWS, X);
        assertTrue(CQuery.from(t1).isAsk());
        assertTrue(CQuery.from(asList(t1, t2)).isAsk());

        assertFalse(CQuery.EMPTY.isAsk());
        assertFalse(CQuery.from(t3).isAsk());
        assertFalse(CQuery.from(asList(t1, t3)).isAsk());
        assertFalse(CQuery.from(asList(t1, t3, t2)).isAsk());
    }

    @Test(dataProvider = "joinClosureData")
    public void testJoinClosure(@Nonnull List<Triple> query, @Nullable Triple triple,
                                @Nonnull Term join, @Nonnull JoinType policy,
                                @Nonnull List<Triple> expected) {
        CQuery cquery = CQuery.from(query);
        CQuery closure = cquery.joinClosure(policy, join, triple);
        assertEquals(closure, expected);

        // caches should still work
        for (int i = 0; i < 4; i++) {
            CQuery otherClosure = cquery.joinClosure(policy, join, triple);
            assertEquals(otherClosure, expected);
        }
    }

    @Test(dataProvider = "joinClosureData")
    public void testParallelJoinClosure(@Nonnull List<Triple> query, @Nullable Triple triple,
                                        @Nonnull Term join, @Nonnull JoinType policy,
                                        @Nonnull List<Triple> expected)
                throws InterruptedException, ExecutionException {
        CQuery cquery = CQuery.from(query);
        ExecutorService e = Executors.newCachedThreadPool();
        ArrayList<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 32; i++) {
                futures.add(e.submit(() -> {
                    CQuery otherClosure = cquery.joinClosure(policy, join, triple);
                    assertEquals(otherClosure, expected);
                }));
            }
        } finally {
            e.shutdown();
            e.awaitTermination(5, TimeUnit.SECONDS);
        }
        for (Future<?> future : futures)
            future.get(); // throws AssertionError's within ExecutionExceptions
    }

    @Test(dataProvider = "triplesJoinClosureData")
    public void testTripleJoinClosure(@Nonnull List<Triple> query, @Nonnull List<Triple> seed,
                                      @Nonnull JoinType policy, @Nonnull List<Triple> expected) {
        CQuery cquery = CQuery.from(query);
        CQuery closure = cquery.joinClosure(seed, policy);
        assertEquals(closure, expected);

        CQuery closureIncluding = cquery.joinClosure(seed, true, policy);
        Set<Triple> expectedIncluding = Sets.newHashSet(expected);
        expectedIncluding.addAll(seed);
        assertEquals(Sets.newHashSet(closureIncluding), expectedIncluding);
    }

    @Test(dataProvider = "triplesJoinClosureData")
    public void testParallelTripleJoinClosure(@Nonnull List<Triple> query,
                                              @Nonnull List<Triple> seed, @Nonnull JoinType pol,
                                              @Nonnull List<Triple> expected) throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        CQuery cquery = CQuery.from(query);
        ArrayList<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 32; i++)
                futures.add(exec.submit(() -> {
                    CQuery closure = cquery.joinClosure(seed, pol);
                    assertEquals(closure, expected);

                    CQuery closureIncluding = cquery.joinClosure(seed, true, pol);
                    Set<Triple> expectedIncluding = Sets.newHashSet(expected);
                    expectedIncluding.addAll(seed);
                    assertEquals(Sets.newHashSet(closureIncluding), expectedIncluding);
                }));
        } finally {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
        for (Future<?> future : futures)
            future.get(); // re-throws AssertionError as ExecutionExceptions
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
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
        for (Future<?> f : futures) f.get(); // throws AssertionError as ExecutionException
    }

    @Test(dataProvider = "streamVarsData")
    public void testStreamVars(@Nonnull List<Triple> query, @Nonnull List<Var> expected) {
        CQuery cQuery = CQuery.from(query);
        Set<Var> actual = cQuery.streamTerms(Var.class).collect(toSet());
        assertEquals(actual, new HashSet<>(expected));
    }

    @Test(dataProvider = "streamVarsData")
    public void testParallelStreamVars(@Nonnull List<Triple> query,
                                       @Nonnull List<Var> expected) throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();
        CQuery cQuery = CQuery.from(query);
        try {
            for (int i = 0; i < 64; i++) {
                futures.add(exec.submit(() -> assertEquals(cQuery.streamTerms(Var.class)
                                                                 .collect(toSet()),
                                                           new HashSet<>(expected))));
            }
        } finally {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
        for (Future<?> f : futures) f.get(); // throws on test failure
    }

    @Test(dataProvider = "streamURIsData")
    public void testStreamURIs(@Nonnull List<Triple> query, @Nonnull List<URI> expected) {
        CQuery cQuery = CQuery.from(query);
        Set<URI> actual = cQuery.streamTerms(URI.class).collect(toSet());
        assertEquals(actual, new HashSet<>(expected));
    }

}