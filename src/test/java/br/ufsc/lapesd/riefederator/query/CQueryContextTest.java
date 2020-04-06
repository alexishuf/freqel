package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
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

        CQuery expected = CQuery.with(new Triple(Alice, p1, x),
                                      new Triple(x, p1, Bob))
                .annotate(Alice, ta1)
                .annotate(Bob, ta2).build();
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
        CQuery expected = CQuery.with(t1, t2)
                .annotate(t1, a1)
                .annotate(t1, a2)
                .annotate(t2, a3).build();
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
        CQuery expected = CQuery.with(triple, new Triple(x, p2, Bob))
                .annotate(x, a1).annotate(Bob, a1).annotate(triple, a2).build();
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
        CQuery expected = CQuery.with(triple, new Triple(x, p2, Bob))
                .annotate(triple, a).annotate(Bob, a).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testAddDistinctModifier() {
        CQuery query;
        query = createQuery(Alice, p1, x, Distinct.REQUIRED);
        assertEquals(query.getModifiers(), singletonList(Distinct.REQUIRED));

        query = createQuery(Distinct.REQUIRED, Alice, p1, x);
        assertEquals(query.getModifiers(), singletonList(Distinct.REQUIRED));

        query = createQuery(Alice, Distinct.REQUIRED, p1, x);
        assertEquals(query.getModifiers(), singletonList(Distinct.REQUIRED));
    }

    @Test
    public void testAddFilters() {
        CQuery query = createQuery(
                Alice, p1, x,
                x, p2, u, SPARQLFilter.build("?u > 23"),
                x, p3, v, SPARQLFilter.build("?v < 23"));
        assertEquals(query.getModifiers(),
                     asList(SPARQLFilter.build("?u > 23"),
                            SPARQLFilter.build("?v < 23")));
    }
}