package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.CQueryContext;
import com.google.errorprone.annotations.Immutable;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

@Test(groups = {"fast"})
public class CQueryContextTest implements TestContext {

    @Immutable
    private static class TermAnn implements TermAnnotation {
    }

    @Immutable
    private static class TripleAnn implements TripleAnnotation {
    }

    @Immutable
    private static class TripleAndTermAnn implements TermAnnotation, TripleAnnotation {
    }

    private static abstract class Context extends CQueryContext {
    }

    @Test
    public void testNoAnnotations() {
        CQuery actual = new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, p1, x,
                              x, p2, Bob);
            }
        }.build();

        CQuery expected = CQuery.from(new Triple(Alice, p1, x), new Triple(x, p2, Bob));
        assertEquals(actual, expected);
    }

    @Test
    public void testTermAnnotations() {
        TermAnn ta1 = new TermAnn(), ta2 = new TermAnn();
        CQuery actual = new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, ta1, p1, x,
                        x, p1, Bob, ta2);
            }
        }.build();

        MutableCQuery expected = new MutableCQuery(
                asList(new Triple(Alice, p1, x), new Triple(x, p1, Bob)));
        expected.annotate(Alice, ta1);
        expected.annotate(Bob, ta2);
        assertEquals(actual, expected);
    }

    @Test
    public void testCannotAnnotateWithoutTerm() {
        TermAnn ta = new TermAnn();
        expectThrows(IllegalStateException.class, () -> new Context() {
            @Override
            public List<Object> queryData() {
                return asList(ta, Alice, p1, x);
            }
        }.build());
    }

    @Test
    public void testCannotAnnotateWithoutTriple() {
        expectThrows(IllegalStateException.class, () -> new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, new TermAnn(), p1, new TripleAnn(), x);
            }
        }.build());
    }

    @Test
    public void testTripleAnnotations() {
        TripleAnn a1 = new TripleAnn(), a2 = new TripleAnn(), a3 = new TripleAnn();
        CQuery actual = new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, p1, x, a1, a2, x, p2, Bob, a3);
            }
        }.build();

        Triple t1 = new Triple(Alice, p1, x);
        Triple t2 = new Triple(x, p2, Bob);
        MutableCQuery expected = new MutableCQuery(asList(t1, t2));
        expected.annotate(t1, a1);
        expected.annotate(t1, a2);
        expected.annotate(t2, a3);
        assertEquals(actual, expected);
        assertEquals(newHashSet(actual.getTripleAnnotations(t1)), newHashSet(a1, a2));
        assertEquals(actual.getTripleAnnotations(t2), Collections.singleton(a3));
    }

    @Test
    public void testAnnotateTermThenTriple() {
        TermAnn a1 = new TermAnn();
        TripleAnn a2 = new TripleAnn();
        CQuery actual = new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, p1, x, a1, a2, x, p2, Bob, a1);
            }
        }.build();

        Triple triple = new Triple(Alice, p1, x);
        MutableCQuery expected = new MutableCQuery(asList(triple, new Triple(x, p2, Bob)));
        expected.annotate(x, a1);
        expected.annotate(Bob, a1);
        expected.annotate(triple, a2);
        assertEquals(actual, expected);
    }

    @Test
    public void testAnnotateTermAndTriple() {
        TripleAndTermAnn a = new TripleAndTermAnn();
        CQuery actual = new Context() {
            @Override
            public List<Object> queryData() {
                return asList(Alice, p1, x, a, x, p2, Bob);
            }
        }.build();

        Triple triple = new Triple(Alice, p1, x);
        MutableCQuery expected = new MutableCQuery(asList(triple, new Triple(x, p2, Bob)));
        expected.annotate(triple, a);
        expected.annotate(Bob, a);
        assertEquals(actual, expected);
    }

    @Test
    public void testAddDistinctModifier() {
        CQuery query;
        query = createQuery(Alice, p1, x, Distinct.INSTANCE);
        assertEquals(query.getModifiers(), singletonList(Distinct.INSTANCE));

        query = createQuery(Distinct.INSTANCE, Alice, p1, x);
        assertEquals(query.getModifiers(), singletonList(Distinct.INSTANCE));

        query = createQuery(Alice, Distinct.INSTANCE, p1, x);
        assertEquals(query.getModifiers(), singletonList(Distinct.INSTANCE));
    }

    @Test
    public void testAddFilters() {
        CQuery query = createQuery(
                Alice, p1, x,
                x, p2, u, SPARQLFilter.build("?u > 23"),
                x, p3, v, SPARQLFilter.build("?v < 23"));
        assertEquals(query.getModifiers(),
                     newHashSet(SPARQLFilter.build("?u > 23"),
                                SPARQLFilter.build("?v < 23")));
    }
}