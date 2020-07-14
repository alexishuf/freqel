package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.impl.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class StarVarIndexTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T")).buildAtom();
    private static final Atom atomCu = Molecule.builder("T").tag(new ColumnTag(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("T").tag(new ColumnTag(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("T").tag(new ColumnTag(co)).buildAtom();
    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);

    private static final IndexedSubset<SPARQLFilter> EMPTY_FILTERS =
            IndexedSet.from(new ArrayList<SPARQLFilter>()).emptySubset();

    @Test
    public void testOrderJoinableSingle() {
        CQuery q = createQuery(x, name, y);
        StarSubQuery star = new StarSubQuery(q.getSet().fullSubset(),
                q.getTermVars().stream().map(Var::getName).collect(toSet()), EMPTY_FILTERS, q);
        assertEquals(StarVarIndex.orderJoinable(singletonList(star)), singletonList(star));
    }

    @Test
    public void testOrderGoodOrder() {
        CQuery q = createQuery(x, name, u, y, nameEx, u);
        StarSubQuery star0 = new StarSubQuery(
                q.getSet().subset(new Triple(x, name, u)),
                newHashSet("x", "u"), EMPTY_FILTERS, q);
        StarSubQuery star1 = new StarSubQuery(
                q.getSet().subset(new Triple(y, nameEx, u)),
                newHashSet("y", "u"), EMPTY_FILTERS, q);
        assertEquals(StarVarIndex.orderJoinable(asList(star0, star1)),
                     asList(star0, star1));
        assertEquals(StarVarIndex.orderJoinable(asList(star1, star0)),
                     asList(star1, star0));
    }

    @Test
    public void testOrderSwap() {
        CQuery q = createQuery(o, age, v,
                               x, name, u,
                               y, nameEx, u, y, knows, o);
        StarSubQuery star0 = new StarSubQuery(
                q.getSet().subset(new Triple(o, age, v)),
                newHashSet("o", "v"), EMPTY_FILTERS, q);
        StarSubQuery star1 = new StarSubQuery(
                q.getSet().subset(new Triple(x, name, u)),
                newHashSet("x", "u"), EMPTY_FILTERS, q);
        StarSubQuery star2 = new StarSubQuery(
                q.getSet().subset(asList(new Triple(y, nameEx, u), new Triple(y, knows, o))),
                newHashSet("y", "u", "o"), EMPTY_FILTERS, q);

        List<StarSubQuery> badOrder = asList(star0, star1, star2);
        List<StarSubQuery> reorder = StarVarIndex.orderJoinable(badOrder);
        assertNotEquals(reorder, badOrder);
        assertTrue(reorder.equals(asList(star0, star2, star1))
                        || reorder.equals(asList(star1, star2, star0))
                        || reorder.equals(asList(star2, star1, star0))
                        || reorder.equals(asList(star2, star0, star1)));
    }

    @Test
    public void testConstructSingleton() {
        CQuery q = createQuery(x, name, y);
        StarSubQuery star0 = new StarSubQuery(q.getSet().subset(new Triple(x, name, y)),
                                              newHashSet("x", "y"), EMPTY_FILTERS, q);
        StarVarIndex index = new StarVarIndex(q, singletonList(star0));
        assertEquals(index.getStarCount(), 1);
        assertSame(index.getStar(0), star0);
        expectThrows(IndexOutOfBoundsException.class, () -> index.getStar(1));
        assertEquals(index.getStarProjection(0), Sets.newHashSet("x", "y"));
        assertEquals(index.getOuterProjection(), Sets.newHashSet("x", "y"));
        assertEquals(index.firstStar("x"), 0);
        assertEquals(index.firstStar("y"), 0);
        assertEquals(index.firstStar("z"), -1);
    }

    @Test
    public void testConstructSingletonProjecting() {
        CQuery q = createQuery(x, name, y, Projection.required("y"));
        StarSubQuery star0 = new StarSubQuery(q.getSet().subset(new Triple(x, name, y)),
                newHashSet("x", "y"), EMPTY_FILTERS, q);
        StarVarIndex index = new StarVarIndex(q, singletonList(star0));
        assertEquals(index.getStarCount(), 1);
        assertSame(index.getStar(0), star0);
        expectThrows(IndexOutOfBoundsException.class, () -> index.getStar(1));
        assertEquals(index.getStarProjection(0), Sets.newHashSet("y")); // x is unused
        assertEquals(index.getOuterProjection(), singleton("y"));
        assertEquals(index.firstStar("x"), 0);
        assertEquals(index.firstStar("y"), 0);
        assertEquals(index.firstStar("z"), -1);
    }

    @Test
    public void testConstructThreeStars() {
        CQuery q = createQuery(o, age, v,
                x, name, u,
                y, nameEx, u, y, knows, o, Projection.advised("v", "u", "o"));
        StarSubQuery star0 = new StarSubQuery(
                q.getSet().subset(new Triple(o, age, v)),
                newHashSet("o", "v"), EMPTY_FILTERS, q);
        StarSubQuery star1 = new StarSubQuery(
                q.getSet().subset(asList(new Triple(y, nameEx, u), new Triple(y, knows, o))),
                newHashSet("y", "u", "o"), EMPTY_FILTERS, q);
        StarSubQuery star2 = new StarSubQuery(
                q.getSet().subset(new Triple(x, name, u)),
                newHashSet("x", "u"), EMPTY_FILTERS, q);

        StarVarIndex index = new StarVarIndex(q, asList(star0, star1, star2));
        assertEquals(index.getStarCount(), 3);
        assertEquals(index.getOuterProjection(), Sets.newHashSet("v", "u", "o"));
        assertSame(index.getStar(0), star0);
        assertSame(index.getStar(1), star1);
        assertSame(index.getStar(2), star2);
        assertEquals(index.firstStar("o"), 0);
        assertEquals(index.firstStar("v"), 0);
        assertEquals(index.firstStar("y"), 1);
        assertEquals(index.firstStar("u"), 1);
        assertEquals(index.firstStar("x"), 2);
        assertEquals(index.getStarProjection(0), newHashSet("o", "v"));
        assertEquals(index.getStarProjection(1), newHashSet("u", "o"));
        assertEquals(index.getStarProjection(2), singleton("u"));
    }

    @Test
    public void testInsertUsedOnURIGeneration() {
        ContextMapping mapping = ContextMapping.builder().beginTable("T")
                .addIdColumn("cu")
                .fallbackPrefix(EX)
                .instancePrefix(EX + "inst/").endTable().build();
        CQuery q = createQuery(
                x, aaT, nameEx, u, aaCu,
                x, aaT, p1,     o, aaCo,
                y, aaT, p1,     o, aaCo,
                y, aaT, age,    v, aaCv, Projection.advised("x", "y", "u", "v"));

        StarSubQuery starX = new StarSubQuery(q.getSet().subset(asList(q.get(0), q.get(1))),
                                              newHashSet("x", "u", "o"), EMPTY_FILTERS, q);
        StarSubQuery starY = new StarSubQuery(q.getSet().subset(asList(q.get(2), q.get(3))),
                                              newHashSet("y", "o", "v"), EMPTY_FILTERS, q);
        StarVarIndex index = new StarVarIndex(q, asList(starX, starY), mapping, false);

        assertEquals(index.getStarProjection(0), newHashSet("x", "u", "o"));
        assertEquals(index.getStarProjection(1), newHashSet("y", "v", "o"));
        assertTrue(index.getOuterProjection().containsAll(newHashSet("x", "y", "u", "v")));
        assertFalse(index.getOuterProjection().contains("o"));
        assertEquals(index.getJoinVars(), singleton("o"));

        assertTrue(index.getIdVar2Column(0).isEmpty());
        assertEquals(new HashSet<>(index.getIdVar2Column(1).values()), singleton(cu));
    }

    @Test
    public void testDoNotInsertUsedOnURIGeneration() {
        ContextMapping mapping = ContextMapping.builder().beginTable("T")
                .addIdColumn("cu")
                .fallbackPrefix(EX)
                .instancePrefix(EX + "inst/").endTable().build();
        CQuery q = createQuery(
                x, aaT, nameEx, u, aaCu,
                x, aaT, p1,     o, aaCo,
                y, aaT, p1,     o, aaCo,
                y, aaT, age,    v, aaCv, Projection.advised("x", "u", "v"));

        StarSubQuery starX = new StarSubQuery(q.getSet().subset(asList(q.get(0), q.get(1))),
                newHashSet("x", "u", "o"), EMPTY_FILTERS, q);
        StarSubQuery starY = new StarSubQuery(q.getSet().subset(asList(q.get(2), q.get(3))),
                newHashSet("y", "o", "v"), EMPTY_FILTERS, q);
        StarVarIndex index = new StarVarIndex(q, asList(starX, starY), mapping, false);

        assertEquals(index.getStarProjection(0), newHashSet("x", "u", "o"));
        assertEquals(index.getStarProjection(1), newHashSet("v", "o"));
        assertTrue(index.getOuterProjection().containsAll(newHashSet("x", "u", "v")));
        assertFalse(index.getOuterProjection().contains("o"));
        assertFalse(index.getOuterProjection().contains("y"));
        assertEquals(index.getJoinVars(), singleton("o"));

        assertTrue(index.getIdVar2Column(0).isEmpty());
        assertTrue(index.getIdVar2Column(1).isEmpty());
    }
}