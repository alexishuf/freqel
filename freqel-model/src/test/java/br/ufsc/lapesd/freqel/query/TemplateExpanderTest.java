package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.model.term.std.TemplateLink;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.PureDescriptive;
import org.testng.annotations.Test;

import java.util.List;

import static br.ufsc.lapesd.freqel.query.TemplateExpander.expandTemplates;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class TemplateExpanderTest implements TestContext {
    private static final URI r1 = new StdURI("https://example.org/r1");

    private static final URI s1 = new StdURI("https://example.org/s1");
    private static final URI s2 = new StdURI("https://example.org/s2");
    private static final URI s3 = new StdURI("https://example.org/s3");

    private static final Var tpl3 = new StdVar("tpl3");
    private static final Var tpl4 = new StdVar("tpl4");
    private static final Var tpl5 = new StdVar("tpl5");
    private static final Var tpl6 = new StdVar("tpl6");
    private static final Var tpl7 = new StdVar("tpl7");

    private static final Atom A1 = new Atom("A1");
    private static final Atom A2 = new Atom("A2");


    @Test
    public void testNoTemplates() {
        assertSame(expandTemplates(CQuery.EMPTY), CQuery.EMPTY);

        CQuery q1 = CQuery.from(new Triple(Alice, p1, Bob));
        CQuery q2 = CQuery.from(new Triple(Alice, p1, x), new Triple(x, p2, Bob));
        assertSame(expandTemplates(q1), q1);
        assertSame(expandTemplates(q2), q2);
    }

    @Test
    public void testNoTemplatesPreserveAnnotations() {
        List<Triple> triples = asList(new Triple(Alice, p1, x),
                                      new Triple(x, p2, Bob));
        PureDescriptive tripleAnn = new PureDescriptive();
        CQuery q = createQuery(
                Alice, AtomInputAnnotation.asRequired(A1, "A1").get(), p1, x,
                x, p2, Bob, AtomAnnotation.of(A2), tripleAnn);
        assertSame(expandTemplates(q), q);
    }

    @Test
    public void testRewriteWithRecursionConflictsAndAnnotations() {
        TemplateLink t2 = new TemplateLink(EX+"t2",
                CQuery.from(new Triple(x, s1, y), new Triple(y, s2, z), new Triple(z, s3, w)),
                x, w);
        TemplateLink t1 = new TemplateLink(EX+"t1",
                CQuery.from(new Triple(x, r1, y), new Triple(y, t2, z)),
                x, z);

        List<Triple> triples = asList(
                /* 0 */ new Triple(Alice, p1, x),
                /* 1 */ new Triple(x, t1, y),
                /* 2 */ new Triple(y, t2, z),
                /* 3 */ new Triple(z, p3, Bob),
                /* 4 */ new Triple(Bob, p3, z)
        );
        PureDescriptive t0Ann = new PureDescriptive(), t3Ann = new PureDescriptive();
        AtomAnnotation AliceAnn = AtomAnnotation.of(A1);
        AtomAnnotation xAnn = AtomAnnotation.of(A1), yAnn = AtomAnnotation.of(A1);
        AtomInputAnnotation zAnn = AtomInputAnnotation.asRequired(A2, "A2").get();

        MutableCQuery query = new MutableCQuery(triples);
        query.annotate(triples.get(0), t0Ann);
        query.annotate(triples.get(3), t3Ann);
        query.annotate(Alice, AliceAnn);
        query.annotate(x, xAnn);
        query.annotate(y, yAnn);
        query.annotate(z, zAnn);
        MutableCQuery expected = new MutableCQuery(asList(
                /*  0 */ new Triple(Alice, p1, x),              /* 0 */
                /*    */ // ~~~ new Triple(x, t1, y),           /* 1 */
                /*  1 */     new Triple(x, r1, tpl3),
                /*    */     // ~~~ new Triple(tpl1, t2, y),
                /*  2 */         new Triple(tpl3, s1, tpl4),
                /*  3 */         new Triple(tpl4, s2, tpl5),
                /*  4 */         new Triple(tpl5, s3, y),
                /*    */ // ~~~ new Triple(y, t2, z),           /* 2 */
                /*  5 */     new Triple(y, s1, tpl6),
                /*  6 */     new Triple(tpl6, s2, tpl7),
                /*  7 */     new Triple(tpl7, s3, z),
                /*  8 */ new Triple(z, p3, Bob),                /* 3 */
                /*  9 */ new Triple(Bob, p3, z)                 /* 4 */
        ));
        expected.annotate(expected.get(0), t0Ann);
        expected.annotate(expected.get(8), t3Ann);
        expected.annotate(Alice, AliceAnn);
        expected.annotate(x, xAnn);
        expected.annotate(y, yAnn);
        expected.annotate(z, zAnn);
        CQuery actual = expandTemplates(query);

        assertEquals(actual, expected);
        expected.forEachTermAnnotation((t, a)
                -> assertTrue(actual.getTermAnnotations(t).contains(a)));
        expected.forEachTripleAnnotation((t, a)
                -> assertTrue(actual.getTripleAnnotations(t).contains(a)));
    }

}