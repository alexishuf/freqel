package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.model.Triple.Position.*;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@SuppressWarnings({"ConstantConditions", "SimplifiableAssertion"})
public class MutableCQueryTest implements TestContext {
    private static final Atom A1 = new Atom("A1");

    private static class Ann implements TripleAnnotation, TermAnnotation, QueryAnnotation {
        int id;

        public Ann(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Ann) && id == ((Ann) o).id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return String.valueOf(id);
        }
    }

    private static class SubAnn extends Ann {
        public SubAnn(int id) {
            super(id);
        }
    }

    private static void assertEqualQueries(@Nonnull CQuery a, @Nonnull CQuery b) {
        assertEquals(a.asList(), b.asList());
        assertEquals(a.getModifiers(), b.getModifiers());
        assertEquals(a.getQueryAnnotations(), b.getQueryAnnotations());
        for (Triple triple : a)
            assertEquals(a.getTripleAnnotations(triple), b.getTripleAnnotations(triple));
        for (Term term : a.attr().allTerms())
            assertEquals(a.getTermAnnotations(term), b.getTermAnnotations(term));
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test(groups = {"fast"})
    public void testOnlyTriples() {
        MutableCQuery q = new MutableCQuery();
        assertTrue(q.isEmpty());
        assertEquals(q.size(), 0);
        assertTrue(q.attr().isAsk());

        assertTrue(q.add(new Triple(x, knows, y)));
        assertFalse(q.isEmpty());
        assertFalse(q.attr().isAsk());
        assertEquals(q.size(), 1);
        assertTrue(q.contains(new Triple(x, knows, y)));
        assertEquals(q.attr().allVarNames(), newHashSet("x", "y"));
        assertEquals(q.attr().allVars(), newHashSet(x, y));
        assertEquals(q.attr().tripleVars(), newHashSet(x, y));
        assertEquals(q.attr().allTerms(), newHashSet(x, knows, y));
        assertEquals(q.attr().tripleTerms(), newHashSet(x, knows, y));

        assertTrue(q.add(new Triple(y, knows, z)));
        assertEquals(q.size(), 2);
        assertEquals(q.attr().allVarNames(), newHashSet("x", "y", "z"));
        assertEquals(q.attr().allVars(), newHashSet(x, y, z));
        assertEquals(q.attr().tripleVars(), newHashSet(x, y, z));
        assertEquals(q.attr().allTerms(), newHashSet(x, knows, y, z));
        assertEquals(q.attr().tripleTerms(), newHashSet(x, knows, y, z));

        assertFalse(q.add(new Triple(x, knows, y)));
        assertEquals(q.size(), 2);
        assertEquals(q.attr().allVarNames(), newHashSet("x", "y", "z"));
        assertEquals(q.attr().allVars(), newHashSet(x, y, z));
        assertEquals(q.attr().tripleVars(), newHashSet(x, y, z));
        assertEquals(q.attr().allTerms(), newHashSet(x, knows, y, z));
        assertEquals(q.attr().tripleTerms(), newHashSet(x, knows, y, z));

        assertEquals(q.attr().publicTripleVarNames(), newHashSet("x", "y", "z"));
        assertEquals(q.attr().publicVarNames(), newHashSet("x", "y", "z"));
    }

    @Test(groups = {"fast"})
    public void testUniqueModifiers() {
        MutableCQuery q = new MutableCQuery();
        assertTrue(q.add(new Triple(x, knows, y)));
        assertEquals(q.attr().publicVarNames(), newHashSet("x", "y"));

        assertTrue(q.mutateModifiers().add(Projection.of("x")));
        assertEquals(q.attr().publicVarNames(), singleton("x"));
        assertEquals(q.getModifiers(), singleton(Projection.of("x")));

        assertFalse(q.mutateModifiers().add(Projection.of("x")));
        assertEquals(q.attr().publicVarNames(), singleton("x"));
        assertEquals(q.getModifiers(), singleton(Projection.of("x")));

        assertTrue(q.mutateModifiers().add(Projection.of("y")));
        assertEquals(q.attr().publicVarNames(), newHashSet("y"));
        assertEquals(q.getModifiers(), singleton(Projection.of("y")));
    }

    @Test(groups = {"fast"})
    public void testMultipleFilters() {
        MutableCQuery q = new MutableCQuery();
        assertTrue(q.add(new Triple(x, age, y)));
        assertTrue(q.mutateModifiers().add(SPARQLFilter.build("?y > 23")));
        assertTrue(q.mutateModifiers().add(SPARQLFilter.build("?y < 35")));
        assertFalse(q.mutateModifiers().add(SPARQLFilter.build("?y < 35")));
        assertEquals(q.getModifiers(),
                newHashSet(SPARQLFilter.build("?y > 23"), SPARQLFilter.build("?y < 35")));
        assertEquals(q.attr().allTerms(), newHashSet(x, age, y, integer(23), integer(35)));
    }

    @Test(groups = {"fast"})
    public void testFilterWithInput() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        assertTrue(q.mutateModifiers().add(SPARQLFilter.build("?u < 23")));
        assertEquals(q.getModifiers().filters(), singleton(SPARQLFilter.build("?u < 23")));

        assertTrue(q.add(new Triple(x, age, u)));
        assertEquals(q.size(), 2);
        assertEquals(q.getModifiers().filters(), singleton(SPARQLFilter.build("?u < 23")));

        // removing the triple does not remove the filter. See sanitizeFilters*()
        assertTrue(q.remove(new Triple(x, age, u)));
        assertEquals(q.getModifiers().filters(), singleton(SPARQLFilter.build("?u < 23")));
    }

    @Test(groups = {"fast"})
    public void testAnnotateTriple() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(x, age, u));
        q.annotate(new Triple(x, knows, y), new Ann(1));
        q.annotate(new Triple(x, knows, y), new Ann(2));
        assertEquals(q.getTripleAnnotations(new Triple(x, knows, y)),
                newHashSet(new Ann(1), new Ann(2)));
        assertEquals(q.getTripleAnnotations(new Triple(x, age, u)), emptySet());
        assertEquals(q.getTripleAnnotations(new Triple(z, idEx, w)), emptySet());
    }

    @Test(groups = {"fast"})
    public void testRemoveTripleAnnotationsWithTriple() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(x, age, u));
        assertTrue(q.annotate(new Triple(x, knows, y), new Ann(1)));
        assertFalse(q.annotate(new Triple(x, knows, y), new Ann(1)));
        assertTrue(q.annotate(new Triple(x, age, u), new Ann(2)));
        assertEquals(q.getTripleAnnotations(new Triple(x, age, u)), singleton(new Ann(2)));

        q.remove(new Triple(x, age, u));
        assertEquals(q.getTripleAnnotations(new Triple(x, age, u)), emptySet());
        assertEquals(q.getTripleAnnotations(new Triple(x, knows, y)), singleton(new Ann(1)));
    }

    @Test(groups = {"fast"})
    public void testDeannotateTriplesByClass() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(x, age, u));
        assertTrue(q.annotate(new Triple(x, knows, y), new Ann(1)));
        assertTrue(q.annotate(new Triple(x, age, u), new SubAnn(2)));
        assertEquals(q.getTripleAnnotations(new Triple(x, knows, y)), singleton(new Ann(1)));
        assertEquals(q.getTripleAnnotations(new Triple(x, age, u)), singleton(new SubAnn(2)));

        assertTrue(q.deannotateTripleIf(SubAnn.class::isInstance));
        assertFalse(q.deannotateTripleIf(SubAnn.class::isInstance));
        assertEquals(q.getTripleAnnotations(new Triple(x, knows, y)), singleton(new Ann(1)));
        assertEquals(q.getTripleAnnotations(new Triple(x, age, u)), emptySet());
    }

    @Test(groups = {"fast"})
    public void testAnnotateTerm() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        assertTrue(q.annotate(x, new Ann(1)));
        assertFalse(q.annotate(x, new Ann(1)));

        assertThrows(IllegalArgumentException.class, () -> q.annotate(u, new Ann(2)));
        q.add(new Triple(x, age, u));
        assertTrue(q.annotate(u, new Ann(3))); //now a valid term
        assertTrue(q.annotate(knows, new Ann(4))); //annotate non-var

        assertEquals(q.getTermAnnotations(x), singleton(new Ann(1)));
        assertEquals(q.getTermAnnotations(y), emptySet());
        assertEquals(q.getTermAnnotations(knows), singleton(new Ann(4)));
        assertEquals(q.getTermAnnotations(age), emptySet());
        assertEquals(q.getTermAnnotations(u), singleton(new Ann(3)));
    }

    @Test(groups = {"fast"})
    public void testRemoveTermRemovesAnnotations() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(y, age, u));
        assertTrue(q.annotate(y, new Ann(1)));
        assertTrue(q.annotate(new Triple(y, age, u), new Ann(2)));
        assertTrue(q.annotate(u, new Ann(3)));

        assertEquals(q.getTermAnnotations(y), singleton(new Ann(1)));
        assertEquals(q.getTripleAnnotations(new Triple(y, age, u)), singleton(new Ann(2)));
        assertEquals(q.getTermAnnotations(u), singleton(new Ann(3)));

        assertTrue(q.remove(new Triple(y, age, u)));
        assertEquals(q.getTermAnnotations(y), singleton(new Ann(1))); //still present in first triple
        assertEquals(q.getTripleAnnotations(new Triple(y, age, u)), emptySet());
        assertEquals(q.getTermAnnotations(u), emptySet());
    }

    @DataProvider
    public static @Nonnull Object[][] sizesData() {
        return Stream.of(0, 1, 2, 3, 4, 8)
                .map(i -> new Object[]{i}).toArray(Object[][]::new);
    }

    private @Nonnull Triple generateConstellationTriple(int index) {
        StdVar core = new StdVar("x" + (index / 3));
        StdURI predicate = new StdURI(EX + "p" + ((index % 3) + 1));
        StdVar obj = new StdVar("o" + ((index % 3) + 1));
        return new Triple(core, predicate, obj);
    }

    private void annotateConstellationTriple(@Nonnull Triple triple, @Nonnull MutableCQuery q) {
        Term s = triple.getSubject(), o = triple.getObject();
        assertTrue(s.isVar());
        assertTrue(o.isVar());
        int block = Integer.parseInt(s.asVar().getName().replaceAll("^x", ""));
        int index = Integer.parseInt(o.asVar().getName().replaceAll("^o", "")) - 1;

        q.annotate(s, new Ann(block));
        q.annotate(o, new Ann(index + 1));
        q.annotate(triple, new Ann(block * 3 + index));
    }

    private void addConstellationTriple(@Nonnull MutableCQuery q, int index) {
        Triple triple = generateConstellationTriple(index);
        q.add(triple);
        annotateConstellationTriple(triple, q);
    }

    private @Nonnull MutableCQuery generateConstellationQuery(int size) {
        assert 2 / 3 == 0; //integer division, right?
        MutableCQuery q = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            Triple triple = generateConstellationTriple(i);
            assertTrue(q.add(triple));
            assertEquals(q.annotate(triple.getSubject(), new Ann(i / 3)), i % 3 == 0);
            assertEquals(q.annotate(triple.getObject(), new Ann((i % 3) + 1)), i < 3);
            q.annotate(triple, new Ann(i));
        }
        return q;
    }

    private void checkConstellationAnnotations(@Nonnull MutableCQuery q, int checkSize) {
        checkConstellationAnnotations(q, checkSize, false);
    }

    private void checkConstellationAnnotations(@Nonnull MutableCQuery q, int checkSize,
                                               boolean forgiveNonConstellation) {
        for (Triple triple : q) {
            Term s = triple.getSubject(), o = triple.getObject();
            int block = -1, off = -1;
            boolean skip = false;
            try {
                assertTrue(s.isVar());
                assertTrue(o.isVar());
                block = Integer.parseInt(s.asVar().getName().replaceAll("^x", ""));
                off = Integer.parseInt(o.asVar().getName().replaceAll("^o", "")) - 1;

            } catch (AssertionError|NumberFormatException e) {
                if (!forgiveNonConstellation)
                    throw e;
                skip = true;
            }
            if (!skip) {
                StdURI expectedPredicate = new StdURI(EX + "p" + (off+1));
                if (triple.getPredicate().equals(expectedPredicate))
                    assertEquals(q.getTripleAnnotations(triple), singleton(new Ann(block*3 + off)));
                else
                    assertTrue(forgiveNonConstellation);
                assertEquals(q.getTermAnnotations(s), singleton(new Ann(block)));
                assertEquals(q.getTermAnnotations(o), singleton(new Ann(off + 1)));
            }
        }
        int foundTriples = 0;
        for (int i = 0; i < checkSize; i++) {
            Triple triple = generateConstellationTriple(i);
            Term s = triple.getSubject(), o = triple.getObject();
            if (q.contains(triple)) {
                ++foundTriples;
                assertEquals(q.getTripleAnnotations(triple), singleton(new Ann(i)));
                assertEquals(q.getTermAnnotations(s), singleton(new Ann(i / 3)));
                assertEquals(q.getTermAnnotations(o), singleton(new Ann((i % 3) + 1)));
            } else {
                assertEquals(q.getTripleAnnotations(triple), emptySet());
            }
        }
        if (!forgiveNonConstellation)
            assertEquals(foundTriples, q.size());
    }

    private void heatCache(@Nonnull MutableCQuery query) {
        CQueryCache c = query.attr();
        assertEquals(c.getSet().size(), query.size());
        assertEquals(c.unmodifiableList(), query);
        assertEquals(c.unmodifiableQueryAnnotations(), query.getQueryAnnotations());

        assertTrue(c.allTerms().containsAll(c.tripleTerms()));
        assertTrue(c.allVars().containsAll(c.tripleVars()));
        assertTrue(c.allVarNames().containsAll(c.tripleVarNames()));
        assertTrue(c.publicVarNames().containsAll(c.publicTripleVarNames()));
        assertTrue(c.inputVarNames().containsAll(c.reqInputVarNames()));
        assertTrue(c.inputVarNames().containsAll(c.optInputVarNames()));

        assertEquals(c.matchedTriples().size(), query.size());
        for (Term term : c.tripleTerms())
            assertFalse(c.triplesWithTerm(term).isEmpty());
        for (Triple triple : query) {
            assertTrue(c.triplesWithTermAt(triple.getSubject(), SUBJ).contains(triple));
            assertTrue(c.triplesWithTermAt(triple.getPredicate(), PRED).contains(triple));
            assertTrue(c.triplesWithTermAt(triple.getObject(), OBJ).contains(triple));
        }
        assertNotNull(c.termAtoms());
        for (Term term : c.allTerms())
            assertNotNull(c.termAtoms(term));
        c.isJoinConnected();
        c.isAsk();
        c.allBound();
        assertTrue(c.limit() >= 0);
        assertEquals(c.queryHash(), query.hashCode());
    }

    private void checkCache(@Nonnull MutableCQuery query) {
        CQueryCache clean = query.createNewCache();
        CQueryCache attr = query.attr();
        assertEquals(attr.getSet(), clean.getSet());
        assertEquals(attr.unmodifiableList(), clean.unmodifiableList());
        assertEquals(attr.unmodifiableQueryAnnotations(), clean.unmodifiableQueryAnnotations());
        assertEquals(attr.tripleTerms(), clean.tripleTerms());
        assertEquals(attr.allTerms(), clean.allTerms());
        assertEquals(attr.tripleVars(), clean.tripleVars());
        assertEquals(attr.tripleVarNames(), clean.tripleVarNames());
        assertEquals(attr.allVars(), clean.allVars());
        assertEquals(attr.allVarNames(), clean.allVarNames());
        assertEquals(attr.publicVarNames(), clean.publicVarNames());
        assertEquals(attr.publicTripleVarNames(), clean.publicTripleVarNames());
        assertEquals(attr.matchedTriples(), clean.matchedTriples());
        for (Term term : clean.tripleTerms()) {
            assertEquals(attr.triplesWithTerm(term), clean.triplesWithTerm(term));
            assertEquals(attr.triplesWithTermAt(term, SUBJ), clean.triplesWithTermAt(term, SUBJ));
            assertEquals(attr.triplesWithTermAt(term, PRED), clean.triplesWithTermAt(term, PRED));
            assertEquals(attr.triplesWithTermAt(term, OBJ), clean.triplesWithTermAt(term, OBJ));
        }
        assertEquals(attr.termAtoms(), clean.termAtoms());
        for (Term term : clean.allTerms())
            assertEquals(attr.termAtoms(term), clean.termAtoms(term));
        assertEquals(attr.isJoinConnected(), clean.isJoinConnected());
        assertEquals(attr.isAsk(), clean.isAsk());
        assertEquals(attr.allBound(), clean.allBound());
        assertEquals(attr.queryHash(), clean.queryHash());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void selfTestGenerateConstellation(int size) {
        for (int i = 0; i < size; i++) {
            MutableCQuery q = generateConstellationQuery(size);
            assertEquals(q.size(), size);
            for (int j = 0; j < i; j++)
                assertEquals(q.get(j), generateConstellationTriple(j));
            checkConstellationAnnotations(q, size);
            heatCache(q);
            checkCache(q); //compares heat cache with freshly recomputed
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveSingleTripleByIndex(int size) {
        for (int i = 0; i < size; i++) {
            MutableCQuery q = generateConstellationQuery(size);
            heatCache(q);
            assertEquals(q.remove(i), generateConstellationTriple(i));
            assertEquals(q.size(), size - 1);
            checkConstellationAnnotations(q, size); //annotations still valid
            checkCache(q);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllTriplesByIndexFromRight(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = size - 1; i >= 0; i--) {
            heatCache(q);
            assertEquals(q.remove(i), generateConstellationTriple(i));
            assertEquals(q.size(), i);
            checkConstellationAnnotations(q, size);
            checkCache(q);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllTriplesByEqualsFromRight(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = size - 1; i >= 0; i--) {
            Triple target = generateConstellationTriple(i);
            assertTrue(q.contains(target));
            heatCache(q);
            assertTrue(q.remove(target));
            assertFalse(q.contains(target));
            assertEquals(q.size(), i);
            checkConstellationAnnotations(q, size);
            checkCache(q);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllTriplesByIndexFromLeft(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = 0; i < size; i++) {
            heatCache(q);
            assertEquals(q.remove(0), generateConstellationTriple(i));
            assertEquals(q.size(), size - (i + 1));
            checkConstellationAnnotations(q, size);
            checkCache(q);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllTriplesByEqualsFromLeft(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = 0; i < size; i++) {
            Triple target = generateConstellationTriple(i);
            assertTrue(q.contains(target));
            heatCache(q);
            assertTrue(q.remove(target));
            assertFalse(q.contains(target));
            assertEquals(q.size(), size - (i + 1));
            checkConstellationAnnotations(q, size);
            checkCache(q);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRandomlyRemoveAllByIndex(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = size - 1; i >= 0; i--) {
            int index = (int) Math.floor(Math.random() * i);
            heatCache(q);
            assertNotNull(q.remove(index));
            checkConstellationAnnotations(q, size);
            checkCache(q);
        }
        assertTrue(q.isEmpty());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void selfTestAlternativeConstellationConstruction(int size) {
        MutableCQuery expected = generateConstellationQuery(size);
        MutableCQuery actual = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            Triple triple = generateConstellationTriple(i);
            heatCache(actual);
            actual.add(triple);
            annotateConstellationTriple(triple, actual);
            checkCache(actual);
        }
        assertEqualQueries(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testBuildConstellationPrepending(int size) {
        MutableCQuery actual = new MutableCQuery();
        for (int i = size - 1; i >= 0; i--) {
            heatCache(actual);
            actual.add(0, generateConstellationTriple(i));
            annotateConstellationTriple(generateConstellationTriple(i), actual);
            checkConstellationAnnotations(actual, size);
            checkCache(actual);
        }
        MutableCQuery expected = generateConstellationQuery(size);
        assertEqualQueries(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testBuildConstellationAppendingByIndex(int size) {
        MutableCQuery actual = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            heatCache(actual);
            actual.add(i, generateConstellationTriple(i));
            annotateConstellationTriple(generateConstellationTriple(i), actual);
            checkConstellationAnnotations(actual, size);
            checkCache(actual);
        }
        MutableCQuery expected = generateConstellationQuery(size);
        assertEqualQueries(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testNoEffectSetOnAllIndices(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        for (int i = 0; i < size; i++) {
            heatCache(q);
            assertEquals(q.set(i, generateConstellationTriple(i)), generateConstellationTriple(i));
            checkCache(q);
            checkConstellationAnnotations(q, size);
        }
        assertEquals(q, generateConstellationQuery(size));
    }

    @Test(dataProvider = "sizesData")
    public void testConcurrentCacheAccess(int size) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 32; i++) {
            MutableCQuery q = generateConstellationQuery(size * 4);
            List<Future<?>> futures = new ArrayList<>(4);
            for (int j = 0; j < 4; j++)
                futures.add(exec.submit(() -> heatCache(q)));
            for (Future<?> future : futures)
                future.get(); // re-throw any AssertionError
            checkCache(q);
            checkConstellationAnnotations(q, size * 4);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveEven(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        List<Triple> evenTriples = new ArrayList<>();
        MutableCQuery expected = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            Triple triple = generateConstellationTriple(i);
            if (i % 2 == 0) {
                evenTriples.add(triple);
            } else {
                expected.add(triple);
                annotateConstellationTriple(triple, expected);
            }
        }
        heatCache(q);
        q.removeAll(evenTriples);
        checkCache(q);
        checkConstellationAnnotations(q, size);
        assertEqualQueries(q, expected);
        checkConstellationAnnotations(q, size);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveIfEven(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        MutableCQuery expected = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            Triple triple = generateConstellationTriple(i);
            if (i % 2 > 0) {
                expected.add(triple);
                annotateConstellationTriple(triple, expected);
            }
        }
        heatCache(q);
        boolean change = q.removeIf(triple -> {
            Term s = triple.getSubject(), o = triple.getObject();
            assertTrue(s.isVar());
            assertTrue(o.isVar());
            int block = Integer.parseInt(s.asVar().getName().replaceAll("^x", ""));
            int offset = Integer.parseInt(o.asVar().getName().replaceAll("^o", "")) - 1;
            return (block * 3 + offset) % 2 == 0;
        });
        assertEquals(change, size > 0);
        checkCache(q);
        checkConstellationAnnotations(q, size);
        assertEqualQueries(q, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRetainEven(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        List<Triple> evenTriples = new ArrayList<>();
        MutableCQuery expected = new MutableCQuery();
        for (int i = 0; i < size; i += 2) {
            Triple triple = generateConstellationTriple(i);
            evenTriples.add(triple);
            expected.add(triple);
            annotateConstellationTriple(triple, expected);
        }
        heatCache(q);
        assertEquals(q.retainAll(evenTriples), size > 1);
        checkCache(q);
        checkConstellationAnnotations(q, size);
        assertEqualQueries(q, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRetainOdd(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        List<Triple> oddTriples = new ArrayList<>();
        MutableCQuery expected = new MutableCQuery();
        for (int i = 1; i < size; i += 2) {
            Triple triple = generateConstellationTriple(i);
            oddTriples.add(triple);
            expected.add(triple);
            annotateConstellationTriple(triple, expected);
        }
        heatCache(q);
        assertEquals(q.retainAll(oddTriples), size > 0);
        checkCache(q);
        checkConstellationAnnotations(q, size);
        assertEqualQueries(q, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testClear(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        heatCache(q);
        q.clear();
        checkCache(q);
        assertEquals(q.size(), 0);
        assertEquals(q, new MutableCQuery());
        //no residual annotations
        for (int i = 0; i < size; i++) {
            Triple triple = generateConstellationTriple(i);
            assertEquals(q.getTripleAnnotations(triple), emptySet());
            assertEquals(q.getTermAnnotations(triple.getSubject()), emptySet());
            assertEquals(q.getTermAnnotations(triple.getObject()), emptySet());
        }
    }

    @Test(groups = {"fast"})
    public void testSetReplaces() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(x, age, u));
        heatCache(q);
        assertEquals(q.set(1, new Triple(y, age, u)), new Triple(x, age, u));
        checkCache(q);
    }

    @Test(groups = {"fast"})
    public void testSetNoOp() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(x, age, u));
        heatCache(q);
        assertEquals(q.set(1, new Triple(x, age, u)), new Triple(x, age, u));
        checkCache(q);
    }

    @Test(groups = {"fast"})
    public void testBadSetIndex() {
        MutableCQuery q = new MutableCQuery();
        assertThrows(IndexOutOfBoundsException.class, () -> q.set(0, new Triple(x, knows, y)));
        q.add(new Triple(x, knows, u));
        assertEquals(q.set(0, new Triple(x, knows, y)), new Triple(x, knows, u));
        assertThrows(IndexOutOfBoundsException.class, () -> q.set(1, new Triple(x, knows, y)));
        assertThrows(IndexOutOfBoundsException.class, () -> q.set(-1, new Triple(x, knows, y)));
    }

    @Test(groups = {"fast"})
    public void testSetRemoves() {
        MutableCQuery q = generateConstellationQuery(3);
        heatCache(q);
        Triple old = q.get(1);
        assertEquals(q.set(1, q.get(0)), old);
        checkCache(q);
        checkConstellationAnnotations(q, 3);

        MutableCQuery expected = generateConstellationQuery(3);
        expected.remove(1);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSetSwapsWithLater() {
        MutableCQuery q = generateConstellationQuery(6);
        heatCache(q);
        Triple old = q.get(1);
        assertEquals(q.set(1, q.get(3)), old);
        checkCache(q);
        checkConstellationAnnotations(q, 6);

        MutableCQuery expected = new MutableCQuery();
        addConstellationTriple(expected, 0);
        addConstellationTriple(expected, 3);
        addConstellationTriple(expected, 2);
        addConstellationTriple(expected, 4);
        addConstellationTriple(expected, 5);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSetSwapsFirstWithLast() {
        MutableCQuery q = generateConstellationQuery(6);
        heatCache(q);
        Triple old = q.get(0);
        assertEquals(q.set(0, q.get(5)), old);
        checkCache(q);
        checkConstellationAnnotations(q, 6);

        MutableCQuery expected = new MutableCQuery();
        addConstellationTriple(expected, 5);
        addConstellationTriple(expected, 1);
        addConstellationTriple(expected, 2);
        addConstellationTriple(expected, 3);
        addConstellationTriple(expected, 4);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSetSwapsWithPrevious() {
        MutableCQuery q = generateConstellationQuery(6);
        heatCache(q);
        Triple old = q.get(3);
        assertEquals(q.set(3, q.get(1)), old);
        checkCache(q);
        checkConstellationAnnotations(q, 6);

        MutableCQuery expected = new MutableCQuery();
        addConstellationTriple(expected, 0);
        addConstellationTriple(expected, 2);
        addConstellationTriple(expected, 1);
        addConstellationTriple(expected, 4);
        addConstellationTriple(expected, 5);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSetSwapsLastWithFirst() {
        MutableCQuery q = generateConstellationQuery(6);
        heatCache(q);
        Triple old = q.get(5);
        assertEquals(q.set(5, q.get(0)), old);
        checkCache(q);
        checkConstellationAnnotations(q, 6);

        MutableCQuery expected = new MutableCQuery();
        addConstellationTriple(expected, 1);
        addConstellationTriple(expected, 2);
        addConstellationTriple(expected, 3);
        addConstellationTriple(expected, 4);
        addConstellationTriple(expected, 0);
        assertEqualQueries(q, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testIteratorRemoveAll(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        Iterator<Triple> it = q.iterator();
        while (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertTrue(q.isEmpty());
        checkConstellationAnnotations(q, size);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testIteratorRemoveOdd(int size) {
        MutableCQuery q = generateConstellationQuery(size), expected = new MutableCQuery();
        Iterator<Triple> it = q.iterator();
        for (int i = 0; it.hasNext(); ++i) {
            it.next();
            if (i % 2 > 0) {
                heatCache(q);
                it.remove();
                checkCache(q);
            } else {
                addConstellationTriple(expected, i);
            }
        }
        assertEqualQueries(q, expected);
        checkConstellationAnnotations(q, size);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testListIteratorRemoveAllLeftToRight(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        ListIterator<Triple> it = q.listIterator();
        while (it.hasNext()) {
            it.next();
            heatCache(q);
            it.remove();
            checkCache(q);
        }
        assertTrue(q.isEmpty());
        checkConstellationAnnotations(q, size);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testListIteratorRemoveAllRightToLeft(int size) {
        MutableCQuery q = generateConstellationQuery(size);
        ListIterator<Triple> it = q.listIterator(size);
        for (int i = size-1; it.hasPrevious(); --i) {
            assertTrue(i >= 0);
            assertEquals(it.previous(), q.get(i));
            heatCache(q);
            it.remove();
            checkCache(q);
        }
        assertTrue(q.isEmpty());
        checkConstellationAnnotations(q, size);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testListIteratorSetAllLeftToRight(int size) {
        if (size <= 1)
            return;
        MutableCQuery q = generateConstellationQuery(size), expected = new MutableCQuery();
        ListIterator<Triple> it = q.listIterator();
        for (int i = 0; it.hasNext(); i++) {
            assertTrue(i < q.size());
            Triple old = it.next();
            String uri = old.getPredicate().asURI().getURI();
            Triple replacement = old.withPredicate(new StdURI(uri.replace(EX + "p", EX + "q")));
            expected.add(replacement);
            annotateConstellationTriple(replacement, expected);
            heatCache(q);
            it.set(replacement);
            checkCache(q);
            q.annotate(replacement, new Ann(i));
            checkCache(q);
            checkConstellationAnnotations(q, size, true);
        }
        assertEqualQueries(q, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testListIteratorSetAllRightToLeft(int size) {
        if (size <= 1)
            return;
        MutableCQuery q = generateConstellationQuery(size), expected = new MutableCQuery();
        ListIterator<Triple> it = q.listIterator(size);
        for (int i = size-1; it.hasPrevious(); i--) {
            assertTrue(i >= 0);
            Triple old = it.previous();
            String uri = old.getPredicate().asURI().getURI();
            Triple replacement = old.withPredicate(new StdURI(uri.replace(EX + "p", EX + "q")));
            expected.add(0, replacement);
            annotateConstellationTriple(replacement, expected);
            heatCache(q);
            it.set(replacement);
            checkCache(q);
            q.annotate(replacement, new Ann(i));
            checkCache(q);
        }
        checkConstellationAnnotations(expected, size, true); //self-test
        checkConstellationAnnotations(q, size, true);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testAddAtEndFromListIterator() {
        MutableCQuery q = generateConstellationQuery(3);
        ListIterator<Triple> it = q.listIterator(q.size());
        heatCache(q);
        it.add(new Triple(x, knows, y));
        checkCache(q);
        it.add(new Triple(x, knows, y)); //no effect
        checkCache(q);

        MutableCQuery expected = generateConstellationQuery(3);
        expected.add(new Triple(x, knows, y));
        checkConstellationAnnotations(q, 3, true);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testAddFromListIterator() {
        MutableCQuery q = generateConstellationQuery(3);
        ListIterator<Triple> it = q.listIterator(q.size());
        assertEquals(it.previousIndex(), 2);
        assertEquals(it.previousIndex(), 2);
        assertSame(it.previous(), q.get(2));
        heatCache(q);
        it.add(new Triple(x, knows, y));
        checkCache(q);
        it.add(new Triple(x, knows, y)); //no effect
        checkCache(q);

        MutableCQuery expected = generateConstellationQuery(3);
        expected.add(2, new Triple(x, knows, y));
        assertEquals(expected.get(3), generateConstellationTriple(2)); //self-test
        checkConstellationAnnotations(q, 3, true);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testAddBeginFromListIterator() {
        MutableCQuery q = generateConstellationQuery(3);
        ListIterator<Triple> it = q.listIterator(0);
        heatCache(q);
        it.add(new Triple(x, knows, y));
        checkCache(q);
        it.add(new Triple(x, knows, y)); //no effect
        checkCache(q);

        MutableCQuery expected = generateConstellationQuery(3);
        expected.add(0, new Triple(x, knows, y));
        assertEquals(expected.get(1), generateConstellationTriple(0)); //self-test
        checkConstellationAnnotations(q, 3, true);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testAddCQuery() {
        MutableCQuery q1 = generateConstellationQuery(6), q2 = generateConstellationQuery(6);
        for (int i = 0; i < 3; i++) {
            q1.remove(3);
            q2.remove(0);
        }
        MutableCQuery merge = new MutableCQuery(q1);
        heatCache(merge);
        merge.mergeWith(q2);
        checkCache(merge);
        assertEqualQueries(merge, generateConstellationQuery(6));
        checkConstellationAnnotations(merge, 6);
    }

    @Test(groups = {"fast"})
    public void testAddEqualQuery() {
        MutableCQuery q1 = generateConstellationQuery(2), q2 = generateConstellationQuery(2);
        heatCache(q1);
        assertFalse(q1.mergeWith(q2));
        checkCache(q1);
        assertEqualQueries(q1, q2);
    }

    @Test(groups = {"fast"})
    public void testAddEmptyQuery() {
        MutableCQuery q1 = generateConstellationQuery(2), empty = new MutableCQuery();
        heatCache(q1);
        assertFalse(q1.mergeWith(empty));
        checkCache(q1);
        assertEqualQueries(q1, generateConstellationQuery(2));
        assertEqualQueries(empty, new MutableCQuery());
    }

    @Test(groups = {"fast"})
    public void testAddOnlyModifiersAndAnnotations() {
        MutableCQuery q1 = generateConstellationQuery(2), q2 = generateConstellationQuery(2);
        q2.annotate(o1, new Ann(666));
        q2.annotate(new Triple(x0, p2, o2), new Ann(777));
        q1.mutateModifiers().add(Projection.of("o1"));
        q2.mutateModifiers().add(Projection.of("o2"));

        heatCache(q1);
        heatCache(q2);
        assertTrue(q1.mergeWith(q2));
        checkCache(q1);
        checkCache(q2); //cache is not invalidated by operation

        MutableCQuery expected = generateConstellationQuery(2);
        expected.annotate(o1, new Ann(666));
        expected.annotate(new Triple(x0, p2, o2), new Ann(777));
        expected.mutateModifiers().add(Projection.of("o1", "o2")); //projections are merged
        assertEqualQueries(q1, expected);
    }

    @Test(groups = {"fast"})
    public void testDeannotateTermByClass() {
        MutableCQuery q = new MutableCQuery(), expected = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.annotate(x, new Ann(1));
        q.annotate(y, new Ann(2));
        q.annotate(y, new SubAnn(3));
        q.annotate(new Triple(x, knows, y), new Ann(3));

        heatCache(q);
        q.deannotateTermIf(SubAnn.class::isInstance);
        checkCache(q);

        expected.add(new Triple(x, knows, y));
        expected.annotate(x, new Ann(1));
        expected.annotate(y, new Ann(2));
        expected.annotate(new Triple(x, knows, y), new Ann(3));
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testDeannotateSpecificTermByClass() {
        MutableCQuery q = new MutableCQuery(), expected = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.annotate(x, new SubAnn(1));
        q.annotate(y, new SubAnn(2));
        q.annotate(x, new Ann(3));
        q.annotate(y, new Ann(4));

        heatCache(q);
        assertTrue(q.deannotateTermIf(y, SubAnn.class::isInstance));
        assertFalse(q.deannotateTermIf(y, SubAnn.class::isInstance));
        checkCache(q);

        expected.add(new Triple(x, knows, y));
        expected.annotate(x, new SubAnn(1));
        expected.annotate(x, new Ann(3));
        expected.annotate(y, new Ann(4));

        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testDeannotateSpecificTripleByClass() {
        MutableCQuery q = new MutableCQuery(), expected = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(y, age, u));
        q.annotate(x, new SubAnn(1));
        q.annotate(y, new SubAnn(2));
        q.annotate(new Triple(x, knows, y), new SubAnn(3));
        q.annotate(new Triple(y, age, u), new SubAnn(4));

        heatCache(q);
        assertTrue(q.deannotateTripleIf(new Triple(y, age, u), SubAnn.class::isInstance));
        assertFalse(q.deannotateTripleIf(new Triple(y, age, u), SubAnn.class::isInstance));
        checkCache(q);

        expected.add(new Triple(x, knows, y));
        expected.add(new Triple(y, age, u));
        expected.annotate(x, new SubAnn(1));
        expected.annotate(y, new SubAnn(2));
        expected.annotate(new Triple(x, knows, y), new SubAnn(3));

        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testShareData() {
        MutableCQuery q1 = new MutableCQuery();
        q1.add(new Triple(x, knows, y));
        heatCache(q1);
        MutableCQuery q2 = new MutableCQuery(q1);
        assertEqualQueries(q1, q2);

        assertTrue(q1.add(new Triple(y, age, u)));
        assertTrue(q1.contains(new Triple(y, age, u)));
        assertFalse(q2.contains(new Triple(y, age, u)));
        assertTrue(q2.contains(new Triple(x, knows, y)));

        checkCache(q1);
        checkCache(q2);
    }

    @Test(groups = {"fast"})
    public void testMergeNoProjectionsOnAdd() {
        MutableCQuery q1 = generateConstellationQuery(2), q2 = generateConstellationQuery(2);
        q1.remove(1);
        q2.remove(0);
        assertEquals(q1.attr().publicVarNames(), newHashSet("x0", "o1"));
        assertEquals(q2.attr().publicVarNames(), newHashSet("x0", "o2"));

        heatCache(q1);
        assertTrue(q1.mergeWith(q2));
        checkCache(q1);
        assertEquals(q1.attr().publicVarNames(), newHashSet("x0", "o1", "o2"));
        assertEqualQueries(q1, generateConstellationQuery(2));
    }

    @Test(groups = {"fast"})
    public void testMergeNoProjectionAndAdvised() {
        MutableCQuery q1 = generateConstellationQuery(1), q2 = generateConstellationQuery(3);
        q2.remove(0);
        q2.mutateModifiers().add(Projection.of("o3"));

        heatCache(q1);
        assertTrue(q1.mergeWith(q2));
        checkCache(q1);
        assertEquals(q2.attr().publicVarNames(), singleton("o3"));

        MutableCQuery expected = generateConstellationQuery(3);
        expected.mutateModifiers().add(Projection.of("x0", "o1", "o3"));
        assertEqualQueries(q1, expected);
    }

    @Test(groups = {"fast"})
    public void testSanitizeFiltersWithoutFilters() {
        MutableCQuery q = generateConstellationQuery(6);
        heatCache(q);
        q.remove(5);
        checkCache(q);
        heatCache(q);
        assertEquals(q.sanitizeFilters(), emptySet());
        checkCache(q);

        checkConstellationAnnotations(q, 6);
        assertEqualQueries(q, generateConstellationQuery(5));
    }

    @Test(groups = {"fast"})
    public void testSanitizeFilters() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(y, age, u));
        q.mutateModifiers().add(SPARQLFilter.build("?u > 23"));
        q.mutateModifiers().add(Ask.INSTANCE);

        assertEquals(q.sanitizeFilters(), emptySet());
        heatCache(q);
        q.remove(1);
        assertEquals(q.sanitizeFilters(), singleton(SPARQLFilter.build("?u > 23")));
        checkCache(q);

        MutableCQuery expected = new MutableCQuery();
        expected.add(new Triple(x, knows, y));
        expected.mutateModifiers().add(Ask.INSTANCE);
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSanitizeFilterStrictly() {
        MutableCQuery q = new MutableCQuery();
        q.add(new Triple(x, knows, y));
        q.add(new Triple(y, age, u));
        q.mutateModifiers().add(SPARQLFilter.build("?u > ?v"));
        q.mutateModifiers().add(Projection.of("y"));

        heatCache(q);
        assertEquals(q.sanitizeFilters(), emptySet());
        checkCache(q); //previous call should not have caused any change

        assertEquals(q.sanitizeFiltersStrict(), singleton(SPARQLFilter.build("?u > ?v")));
        checkCache(q);
        MutableCQuery expected1 = new MutableCQuery();
        expected1.add(new Triple(x, knows, y));
        expected1.add(new Triple(y, age, u));
        expected1.mutateModifiers().add(Projection.of("y"));
        assertEqualQueries(q, expected1);

        q.mutateModifiers().add(SPARQLFilter.build("?u > ?v")); // re-add the filter

        heatCache(q);
        assertTrue(q.remove(new Triple(y, age, u)));
        checkCache(q);

        // at this stage, even a non-strict sanitize removes the filter
        heatCache(q);
        assertEquals(q.sanitizeFilters(), singleton(SPARQLFilter.build("?u > ?v")));
        checkCache(q);
        MutableCQuery expected2 = new MutableCQuery();
        expected2.add(new Triple(x, knows, y));
        expected2.mutateModifiers().add(Projection.of("y"));
        assertEqualQueries(q, expected2);
    }

    @Test(groups = {"fast"})
    public void testSanitizeProjectionNoProjection() {
        MutableCQuery q = generateConstellationQuery(3);
        heatCache(q);
        assertFalse(q.sanitizeProjection());
        checkCache(q);
        assertFalse(q.sanitizeProjectionStrict());
        checkCache(q);
    }

    @Test(groups = {"fast"})
    public void testSanitizeProjectionNoWork() {
        MutableCQuery q = generateConstellationQuery(3);
        assertTrue(q.mutateModifiers().add(Projection.of("o1", "o2")));
        heatCache(q);
        assertFalse(q.sanitizeProjection());
        checkCache(q);
        assertFalse(q.sanitizeProjectionStrict());
        checkCache(q);
    }

    @Test(groups = {"fast"})
    public void testSanitizeProjectionNonStrict() {
        MutableCQuery q = generateConstellationQuery(3);
        assertTrue(q.mutateModifiers().add(Projection.of("o1", "y")));
        assertEquals(q.attr().publicVarNames(), newHashSet("o1", "y"));

        heatCache(q);
        assertTrue(q.sanitizeProjection());
        checkCache(q);
        assertEquals(q.attr().publicVarNames(), singleton("o1"));

        MutableCQuery expected = generateConstellationQuery(3);
        expected.mutateModifiers().add(Projection.of("o1"));
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testSanitizeProjectionStrict() {
        MutableCQuery q = generateConstellationQuery(3);
        assertTrue(q.mutateModifiers().add(Projection.of("o1", "y")));
        assertTrue(q.mutateModifiers().add(SPARQLFilter.build("?o1 > ?y")));
        assertEquals(q.attr().publicVarNames(), newHashSet("o1", "y"));

        heatCache(q);
        assertFalse(q.sanitizeProjection()); //no effect
        checkCache(q);
        assertEquals(q.attr().publicVarNames(), newHashSet("o1", "y"));

        heatCache(q);
        assertTrue(q.sanitizeProjectionStrict()); //removes y
        checkCache(q);
        assertEquals(q.attr().publicVarNames(), singleton("o1"));


        MutableCQuery expected = generateConstellationQuery(3);
        expected.mutateModifiers().add(Projection.of("o1"));
        expected.mutateModifiers().add(SPARQLFilter.build("?o1 > ?y"));
        assertEqualQueries(q, expected);
    }

    @Test(groups = {"fast"})
    public void testCopyAnnotations() {
        MutableCQuery q = generateConstellationQuery(6);

        //remove all annotations
        heatCache(q);
        q.attr().allTerms().forEach(q::deannotate);
        q.asList().forEach(q::deannotate);
        checkCache(q);
        assertTrue(q.attr().allTerms().stream().map(q::getTermAnnotations).allMatch(Set::isEmpty));
        assertTrue(q.stream().map(q::getTripleAnnotations).allMatch(Set::isEmpty));

        assertTrue(q.copyTermAnnotations(generateConstellationQuery(6)));
        assertTrue(q.copyTripleAnnotations(generateConstellationQuery(6)));

        assertEqualQueries(q, generateConstellationQuery(6));
    }

    @Test(groups = {"fast"})
    public void testAddPath() {
        MutableCQuery query = new MutableCQuery();
        query.add(new Triple(x, age, lit(22)));
        query.add(x, SimplePath.fromTerms(knows, isPrimaryTopicOf, title), y);
        query.add(x, SimplePath.fromTerms(author, genre), z);
        query.annotate(y, AtomInputAnnotation.asRequired(A1, "a1").get());

        assertEquals(query.size(), 6);
        assertTrue(query.contains(new Triple(x, age, lit(22))));
        assertTrue(query.attr().allVars().containsAll(Sets.newHashSet(x, y, z)));
        assertEquals(query.attr().allVars().size(), 3/*x, y, z*/ + 3 /*hidden*/);
        assertEquals(query.getTermAnnotations(y),
                singleton(AtomInputAnnotation.asRequired(A1, "a1").get()));

        HashSet<Triple> set = new HashSet<>(query.attr().getSet());
        assertTrue(set.remove(new Triple(x, age, lit(22))));
        Triple kt, it, tt, at, gt;
        kt = set.stream().filter(t -> t.getPredicate().equals(knows)).findFirst().orElse(null);
        it = set.stream().filter(t -> t.getPredicate().equals(isPrimaryTopicOf)).findFirst().orElse(null);
        tt = set.stream().filter(t -> t.getPredicate().equals(title)).findFirst().orElse(null);
        at = set.stream().filter(t -> t.getPredicate().equals(author)).findFirst().orElse(null);
        gt = set.stream().filter(t -> t.getPredicate().equals(genre)).findFirst().orElse(null);
        assertNotNull(kt);
        assertNotNull(it);
        assertNotNull(tt);
        assertNotNull(at);
        assertNotNull(gt);
        assertEquals(kt.getSubject(), x);
        assertEquals(kt.getObject(), it.getSubject());
        assertEquals(it.getObject(), tt.getSubject());
        assertEquals(tt.getObject(), y);

        assertEquals(at.getSubject(), x);
        assertEquals(at.getObject(), gt.getSubject());
        assertEquals(gt.getObject(), z);

        assertNotEquals(at.getObject(), kt.getObject());
        assertNotEquals(at.getObject(), it.getObject());
    }

    @Test(groups = {"fast"})
    public void testModifierRemoveIfCopies() {
        MutableCQuery q1 = generateConstellationQuery(3);
        assertTrue(q1.mutateModifiers().add(Projection.of("x")));
        assertTrue(q1.mutateModifiers().add(Limit.of(23)));

        MutableCQuery q2 = new MutableCQuery(q1);
        assertTrue(q2.mutateModifiers().removeIf(Projection.class::isInstance));

        MutableCQuery expected1 = generateConstellationQuery(3);
        assertTrue(expected1.mutateModifiers().add(Projection.of("x")));
        assertTrue(expected1.mutateModifiers().add(Limit.of(23)));
        assertEqualQueries(q1, expected1);

        MutableCQuery expected2 = generateConstellationQuery(3);
        assertTrue(expected2.mutateModifiers().add(Limit.of(23)));
        assertEqualQueries(q2, expected2);
    }

    @Test(groups = {"fast"})
    public void testModifierIteratorRemoveCopies() {
        MutableCQuery q1 = generateConstellationQuery(3), expected = generateConstellationQuery(3);
        q1.mutateModifiers().add(Projection.of("x"));
        q1.mutateModifiers().add(Limit.of(23));
        expected.mutateModifiers().add(Projection.of("x"));
        expected.mutateModifiers().add(Limit.of(23));

        MutableCQuery q2 = new MutableCQuery(q1);
        Iterator<Modifier> it = q2.mutateModifiers().iterator();
        assertTrue(it.hasNext());
        Modifier removed = it.next();
        it.remove();

        assertEqualQueries(q1, expected);
        assertFalse(q2.getModifiers().contains(removed));
        assertEquals(q2.getModifiers().size(), 1);
    }

    @Test(groups = {"fast"})
    public void testBlockUnsafeMerge() {
        MutableCQuery a = generateConstellationQuery(3), b = generateConstellationQuery(6);
        assertTrue(b.mutateModifiers().add(Optional.EXPLICIT));

        expectThrows(UnsafeMergeException.class, () -> a.mergeWith(b));
        assertEquals(a, generateConstellationQuery(3)); // no side effect remains
    }

    @Test(invocationCount = 8)
    public void testConcurrentCopy() throws Exception {
        MutableCQuery original = generateConstellationQuery(3);
        original.mutateModifiers().add(Optional.EXPLICIT);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<CQuery>> futures1 = new ArrayList<>();
        List<Future<CQuery>> futures2 = new ArrayList<>();
        try {
            IntStream.range(0, 256).forEach(i -> {
                futures1.add(executor.submit(() -> {
                    MutableCQuery copy = new MutableCQuery(original);
                    copy.mutateModifiers().add(Limit.of(i+1));
                    return copy;
                }));
                futures2.add(executor.submit(() -> {
                    MutableCQuery copy = new MutableCQuery(original);
                    copy.mutateModifiers().remove(Optional.EXPLICIT);
                    return copy;
                }));
            });
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        for (int i = 0, size = futures1.size(); i < size; i++) {
            Future<CQuery> future = futures1.get(i);
            CQuery actual = future.get();
            MutableCQuery expected = generateConstellationQuery(3);
            expected.mutateModifiers().add(Limit.of(i+1));
            assertEquals(actual, expected);
        }
        for (Future<CQuery> future : futures2)
            assertEquals(future.get(), generateConstellationQuery(3));
        assertEquals(original, generateConstellationQuery(3));
    }

}