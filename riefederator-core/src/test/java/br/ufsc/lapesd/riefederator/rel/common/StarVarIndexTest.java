package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.context.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.rel.sql.impl.DefaultSqlTermWriter;
import br.ufsc.lapesd.riefederator.rel.sql.impl.FilterSelector;
import br.ufsc.lapesd.riefederator.rel.sql.impl.SqlSelectorFactory;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.IntStream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet.newIndexedSet;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class StarVarIndexTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column cp = new Column("T", "cp");
    private static final Column cv1 = new Column("T", "cv1");
    private static final Column cv2 = new Column("T", "cv2");

    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");
    private static final Column ko = new Column("U", "ko");

    private static final Column ju = new Column("V", "ju");
    private static final Column jv = new Column("V", "jv");
    private static final Column jo = new Column("V", "jo");

    private static final Column lu1 = new Column("W", "lu1");
    private static final Column lu2 = new Column("W", "lu2");
    private static final Column lv  = new Column("W", "lv");

    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T"))
                                                           .tag(ColumnsTag.direct(cu))
                                                           .buildAtom();
    private static final Atom atomU = Molecule.builder("U").tag(new TableTag("U"))
                                                           .tag(ColumnsTag.direct(ku))
                                                           .buildAtom();
    private static final Atom atomV = Molecule.builder("V").tag(new TableTag("V"))
                                                           .tag(ColumnsTag.direct(ju))
                                                           .buildAtom();
    private static final Atom atomW = Molecule.builder("W").tag(new TableTag("W"))
                                                           .tag(new ColumnsTag(asList(lu1, lu2)))
                                                           .buildAtom();

    private static final Atom atomCu = Molecule.builder("T").tag(ColumnsTag.direct(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("T").tag(ColumnsTag.direct(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("T").tag(ColumnsTag.direct(co)).buildAtom();
    private static final Atom atomCp = Molecule.builder("T").tag(new ColumnsTag(singletonList(cp))).buildAtom();
    private static final Atom atomCvv = Molecule.builder("T").tag(new ColumnsTag(asList(cv1, cv2))).buildAtom();

    private static final Atom atomKu = Molecule.builder("U").tag(ColumnsTag.direct(ku)).buildAtom();
    private static final Atom atomKv = Molecule.builder("U").tag(ColumnsTag.direct(kv)).buildAtom();
    private static final Atom atomKo = Molecule.builder("U").tag(ColumnsTag.direct(ko)).buildAtom();

    private static final Atom atomJu = Molecule.builder("V").tag(ColumnsTag.direct(ju)).buildAtom();
    private static final Atom atomJv = Molecule.builder("V").tag(ColumnsTag.direct(jv)).buildAtom();
    private static final Atom atomJo = Molecule.builder("V").tag(ColumnsTag.direct(jo)).buildAtom();

    private static final Atom atomLu = Molecule.builder("W").tag(new ColumnsTag(asList(lu1, lu2))).buildAtom();
    private static final Atom atomLv = Molecule.builder("W").tag(ColumnsTag.direct(lv)).buildAtom();

    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaU = AtomAnnotation.of(atomU);
    private static final AtomAnnotation aaV = AtomAnnotation.of(atomV);
    private static final AtomAnnotation aaW = AtomAnnotation.of(atomW);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);
    private static final AtomAnnotation aaCp = AtomAnnotation.of(atomCp);
    private static final AtomAnnotation aaCvv = AtomAnnotation.of(atomCvv);
    private static final AtomAnnotation aaKu = AtomAnnotation.of(atomKu);
    private static final AtomAnnotation aaKv = AtomAnnotation.of(atomKv);
    private static final AtomAnnotation aaKo = AtomAnnotation.of(atomKo);
    private static final AtomAnnotation aaJu = AtomAnnotation.of(atomJu);
    private static final AtomAnnotation aaJv = AtomAnnotation.of(atomJv);
    private static final AtomAnnotation aaJo = AtomAnnotation.of(atomJo);
    private static final AtomAnnotation aaLu = AtomAnnotation.of(atomLu);
    private static final AtomAnnotation aaLv = AtomAnnotation.of(atomLv);

    private static final IndexSubset<SPARQLFilter> EMPTY_FILTERS =
            FullIndexSet.from(new ArrayList<SPARQLFilter>()).emptySubset();

    private static final ContextMapping mappingIdU = ContextMapping.builder()
            .beginTable("T")
                .fallbackPrefix(EX).instancePrefix(EX+"inst/").addIdColumn("cu")
            .endTable()
            .beginTable("U")
                .fallbackPrefix(EX).instancePrefix(EX+"inst/").addIdColumn("ku")
            .endTable()
            .beginTable("V")
                .fallbackPrefix(EX).instancePrefix(EX+"inst/").addIdColumn("ju")
            .endTable()
            .beginTable("W")
                .fallbackPrefix(EX).instancePrefix(EX+"inst/").addIdColumn("lu1").addIdColumn("lu2")
            .endTable().build();
    private static final ContextMapping mappingIdVV = ContextMapping.builder()
            .beginTable("T")
            .fallbackPrefix(EX).instancePrefix(EX+"inst/").addIdColumn("cv1").addIdColumn("cv2")
            .endTable().build();

    private static final SqlSelectorFactory selectorFactory
            = new SqlSelectorFactory(DefaultSqlTermWriter.INSTANCE);

    @Test
    public void testOrderJoinableSingle() {
        CQuery q = createQuery(x, name, y);
        IndexSet<String> allVars = q.attr().tripleVarNames();

        StarSubQuery star = new StarSubQuery(q.attr().getSet().fullSubset(),
                allVars.fullSubset(), EMPTY_FILTERS, q);
        assertEquals(StarVarIndex.orderJoinable(singletonList(star)), singletonList(star));
    }

    @Test
    public void testOrderGoodOrder() {
        CQuery q = createQuery(x, name, u, y, nameEx, u);
        IndexSet<String> allVars = newIndexedSet("x", "y", "u");
        StarSubQuery star0 = new StarSubQuery(
                q.attr().getSet().subset(new Triple(x, name, u)),
                allVars.subset(asList("x", "u")), EMPTY_FILTERS, q);
        StarSubQuery star1 = new StarSubQuery(
                q.attr().getSet().subset(new Triple(y, nameEx, u)),
                allVars.subset(asList("y", "u")), EMPTY_FILTERS, q);
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
        IndexSet<String> allVars = newIndexedSet("o", "v", "x", "u", "y");
        StarSubQuery star0 = new StarSubQuery(
                q.attr().getSet().subset(new Triple(o, age, v)),
                allVars.subset(asList("o", "v")), EMPTY_FILTERS, q);
        StarSubQuery star1 = new StarSubQuery(
                q.attr().getSet().subset(new Triple(x, name, u)),
                allVars.subset(asList("x", "u")), EMPTY_FILTERS, q);
        StarSubQuery star2 = new StarSubQuery(
                q.attr().getSet().subset(asList(new Triple(y, nameEx, u), new Triple(y, knows, o))),
                allVars.subset(asList("y", "u", "o")), EMPTY_FILTERS, q);

        List<StarSubQuery> badOrder = asList(star0, star1, star2);
        List<StarSubQuery> reorder = StarVarIndex.orderJoinable(badOrder);
        assertNotEquals(reorder, badOrder);
        assertTrue(reorder.equals(asList(star0, star2, star1))
                || reorder.equals(asList(star1, star2, star0))
                || reorder.equals(asList(star2, star1, star0))
                || reorder.equals(asList(star2, star0, star1)));
    }

    private void assertValidIndex(@Nonnull StarVarIndex index, boolean distinctOuterColumns) {
        //every query triple is in a single star
        for (Triple triple : index.getQuery()) {
            int found = 0;
            for (int i = 0, size = index.getStarCount(); i < size; i++)
                found += index.getStar(i).getTriples().contains(triple) ? 1 : 0;
            assertEquals(found, 1, triple+" should be found in exactly one star");
        }
        // every pending triple is in the star and in the query
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            for (Triple triple : index.getPendingTriples(i)) {
                assertTrue(index.getStar(i).getTriples().contains(triple));
                assertTrue(index.getQuery().contains(triple));
            }
        }
        //every pending filter for a star belongs to that star
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            for (SPARQLFilter filter : index.getPendingFilters(i)) {
                assertTrue(index.getQuery().getModifiers().contains(filter));
                assertTrue(index.getStar(i).getFilters().contains(filter));
            }
        }
        // test get all filters
        assertEquals(index.getAllFilters(),
                index.getQuery().getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                                               .map(m -> (SPARQLFilter)m).collect(toSet()));
        // test get all sparql variables
        assertEquals(index.getAllSparqlVars(), index.getQuery().attr().tripleVarNames());
        // every cross-star filter is listed
        Set<SPARQLFilter> crossStarFilters = index.getQuery().getModifiers().stream()
                .filter(SPARQLFilter.class::isInstance)
                .map(m -> (SPARQLFilter) m)
                .filter(f -> IntStream.range(0, index.getStarCount())
                        .noneMatch(i -> index.getStar(i).getFilters().contains(f)))
                .collect(toSet());
        assertEquals(index.getCrossStarFilters(), crossStarFilters);

        //every outer projected var must be in a star projection
        List<Column> outerColumns = new ArrayList<>();
        for (String v : index.getOuterProjection()) {
            boolean found = false;
            for (int i = 0, size = index.getStarCount(); !found && i < size; i++)
                found = index.getProjection(i).contains(v);
            assertTrue(found, "outer projected var "+v+" not in any star projection");
            outerColumns.add(index.getColumn(v));
        }
        assertTrue(outerColumns.stream().noneMatch(Objects::isNull),
                "There are outer variables without a Column");
        if (distinctOuterColumns) {
            //usually there are no duplicate columns in the outer
            assertEquals(outerColumns.size(), new HashSet<>(outerColumns).size(),
                    "There are duplicate columns in the outer projection: "+outerColumns);
        }
        //check var -- column relation within each star
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            SortedSet<String> vars = index.getProjection(i);
            List<Column> innerColumns = new ArrayList<>();
            for (String v : vars)
                innerColumns.add(index.getColumn(v));
            assertTrue(innerColumns.stream().noneMatch(Objects::isNull),
                    "Star "+i+" has projected variables without a column");
            assertEquals(innerColumns.size(), new HashSet<>(innerColumns).size(),
                    "Projection for star "+i+" has duplicate columns");
        }

        // check that all pending triples and filters are in the original query
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            assertTrue(index.getQuery().getModifiers().containsAll(index.getPendingFilters(i)),
                    "Some pending filters are not in the input query!");
            assertTrue(index.getQuery().containsAll(index.getPendingTriples(i)),
                    "Some pending triples are not in the input query!");
        }
        // check that all pending triples and filters have the columns required for execution
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            for (SPARQLFilter filter : index.getPendingFilters(i)) {
                for (Var term : filter.getVars()) {
                    for (Column column : index.getStar(i).getAllColumns(term)) {
                        assertTrue(index.getProjection(i).stream()
                                        .anyMatch(v3 -> index.getColumn(v3).equals(column)),
                                term+" from "+filter+" has no value for column "+column+
                                        " in star projection");
                        assertTrue(index.getOuterProjection().stream()
                                        .anyMatch(v3 -> index.getColumn(v3).equals(column)),
                                term+" from "+filter+" has no value for column "+column+
                                        " in outer projection");
                    }
                }
            }
            for (Triple triple : index.getPendingTriples(i)) {
                for (Column column : index.getStar(i).getAllColumns(triple.getObject())) {
                    assertTrue(index.getProjection(i).stream()
                                    .anyMatch(v -> index.getColumn(v).equals(column)),
                            triple.getObject()+" needs column "+column+" not projected by star");
                    assertTrue(index.getOuterProjection().stream()
                                    .anyMatch(v -> index.getColumn(v).equals(column)),
                            triple.getObject()+" needs column "+column+" not outer projected");
                }
            }
        }
        // check that the number of selectors matches number of non-pending triples/filters
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            StarSubQuery star =  index.getStar(i);
            int doneFilters = star.getFilters().size() - index.getPendingFilters(i).size();
            int doneTriples = star.getTriples().size() - index.getPendingTriples(i).size();
            List<Selector> selectors = index.getSelectors(i);
            assertTrue(selectors.size() <=doneFilters + doneTriples);
            assertEquals(selectors.stream().filter(FilterSelector.class::isInstance).count(),
                    doneFilters);
        }

        //check that for every result sparql variable there is a pending triple with it
        IndexSubset<Triple> pendingTriples = index.getQuery().attr().getSet().emptySubset();
        for (int i = 0, size = index.getStarCount(); i < size; i++)
            pendingTriples.addAll(index.getPendingTriples(i));

        Set<String> missingResultVars = index.getQuery().attr().publicTripleVarNames().stream()
                .filter(v -> pendingTriples.stream().noneMatch(t -> t.contains(new StdVar(v))))
                .filter(v -> {
                    for (int i = 0, size = index.getStarCount(); i < size; i++) {
                        if (index.getStar(i).isCore(v))
                            return false;
                    }
                    return true;
                })
                .collect(toSet());
        assertEquals(missingResultVars, emptySet(), "There are result vars that are not " +
                "present in any pending triple nor are star cores");
    }

    private void checkJoinVars(@Nonnull StarVarIndex index, @Nonnull Term core1,
                               @Nonnull Term core2,
                               @Nonnull List<Column> core1Columns,
                               @Nonnull List<Column> core2Columns) {
        int core1Idx = index.findStar(core1), core2Idx = index.findStar(core2);
        assert core2Idx > core1Idx : "Bad checkJoinVars call";
        assert core1Columns.size() == core2Columns.size();

        Set<StarJoin> joins = index.getJoins(core2Idx);
        Set<Column> visited2 = new HashSet<>();
        for (int i = 0; i < core1Columns.size(); i++) {
            Column ec1 = core1Columns.get(i), ec2 = core2Columns.get(i);
            int encounters = 0;
            for (StarJoin join : joins) {
                assertEquals(join.getStarIdx2(), core2Idx);
                if (join.getStarIdx1() != core1Idx) continue;
                Column ac1 = index.getColumn(join.getVar1());
                Column ac2 = index.getColumn(join.getVar2());
                visited2.add(ac2);
                if (ac2.equals(ec2)) {
                    ++encounters;
                    assertEquals(ac1, ec1);
                }
            }
            assertEquals(encounters, 1, "Column "+ec2+" of star "+core2Idx+" with core "
                    +core2+" is joined "+encounters+" times");
            assertEquals(visited2, new HashSet<>(core2Columns));
        }
    }

    private @Nonnull Set<Column> exposedOuterColumns(@Nonnull StarVarIndex index) {
        return index.getOuterProjection().stream().map(index::getColumn).collect(toSet());
    }
    private @Nonnull Set<Column> exposedColumns(@Nonnull StarVarIndex index,
                                                @Nonnull Term core) {
        return index.getProjection(index.findStar(core)).stream().map(index::getColumn)
                .collect(toSet());
    }
    private @Nonnull Set<Column> exposedColumns(@Nonnull StarVarIndex index) {
        Set<Column> set = new HashSet<>();
        index.getOuterProjection().stream().map(index::getColumn).forEach(set::add);
        for (int i = 0, size = index.getStarCount(); i < size; i++)
            index.getProjection(i).stream().map(index::getColumn).forEach(set::add);
        return set;
    }

    @Test
    public void testSingleTriple() {
        CQuery q = createQuery(x, aaT, name, u, aaCu);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 1);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet());
        assertEquals(index.getStar(0).getVarNames(), newHashSet("x", "u"));

        assertEquals(index.getProjection(0).size(), 1);
        assertEquals(index.getOuterProjection().size(), 1);
        assertValidIndex(index, true);
        assertEquals(exposedColumns(index), newHashSet(cu));
        assertEquals(exposedColumns(index, x), newHashSet(cu));
    }

    @Test
    public void testSingleStar() {
        CQuery q = createQuery(x, aaT, nameEx, v, aaCv,
                x, aaT, ageEx,  o, aaCo);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 1);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet());

        assertEquals(index.getOuterProjection().size(), 3);
        assertEquals(index.getProjection(0).size(), 3);
        assertValidIndex(index, true);
        assertEquals(exposedColumns(index), newHashSet(cu, cv, co));
        assertEquals(exposedColumns(index, x), newHashSet(cu, cv, co));
    }

    @Test
    public void testSingleStarHideSubject() {
        CQuery q = createQuery(x, aaT, nameEx, v, aaCv,
                x, aaT, ageEx,  o, aaCo, Projection.of("v", "o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 1);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet());

        assertEquals(index.getOuterProjection().size(), 2);
        assertEquals(index.getProjection(0).size(), 2);
        assertValidIndex(index, true);
        assertEquals(exposedColumns(index), newHashSet(cv, co));
        assertEquals(exposedColumns(index, x), newHashSet(cv, co));
    }

    @Test
    public void testTwoColumnsPerVar() {
        CQuery q = createQuery(x, aaT, nameEx, v, aaCvv,
                x, aaT, ageEx,  o, aaCo, Projection.of("v", "o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 1);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet());

        assertEquals(index.getOuterProjection().size(), 3);
        assertEquals(index.getProjection(0).size(), 3);
        assertValidIndex(index, true);
        assertEquals(exposedColumns(index), newHashSet(cv1, cv2, co));
        assertEquals(exposedColumns(index, x), newHashSet(cv1, cv2, co));
    }

    @Test
    public void testExposeColumnUsedInFilter() {
        CQuery q = createQuery(x, aaT, nameEx, TestContext.u, aaCu,
                x, aaT, ageEx, v, aaCv, SPARQLFilter.build("REGEX(?v, \"2.*\")"),
                Projection.of("u"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 1);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet());
        assertEquals(index.getStar(0).getFilters(),
                singleton(SPARQLFilter.build("REGEX(?v, \"2.*\")")));

        assertValidIndex(index, true);
        // cv is exposed as the filter cannot be converted into a WHERE clause
        assertEquals(exposedColumns(index), newHashSet(cu, cv));
        assertEquals(exposedColumns(index, x), newHashSet(cu, cv));
    }

    @Test
    public void testTwoStars() {
        CQuery q = createQuery(x, aaT, nameEx,        u, aaCu,
                y, aaT, nameEx,        u, aaCu,
                y, aaT, ageEx,         v, aaCv, SPARQLFilter.build("?v < 23"),
                y, aaT, university_id, o, aaCo);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 2);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet().subset(q.get(0)));
        IndexSubset<Triple> star1Triples = q.attr().getSet().fullSubset();
        star1Triples.remove(q.get(0));
        assertEquals(index.getStar(1).getTriples(), star1Triples);

        assertValidIndex(index, true);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv, co));

        assertEquals(index.getOuterProjection().size(), 3);
        assertEquals(exposedColumns(index), newHashSet(cu, cv, co));
    }


    @Test
    public void testTwoStarsWithProjection() {
        CQuery q = createQuery(x, aaT, nameEx  , u, aaCu,
                y, aaT, nameEx,   u, aaCu,
                y, ageEx,         v, aaCv, SPARQLFilter.build("REGEX(?v, \"2.*\")"),
                y, university_id, o, aaCo, Projection.of("o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 2);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet().subset(q.get(0)));
        IndexSubset<Triple> star1Triples = q.attr().getSet().fullSubset();
        star1Triples.remove(q.get(0));
        assertEquals(index.getStar(1).getTriples(), star1Triples);

        assertValidIndex(index, true);
        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv, co));
        assertEquals(exposedColumns(index),
                newHashSet(cu /*required by join */,
                        cv /* required by filter */,
                        co /* required by projection */));
        assertEquals(index.getOuterProjection().size(), 2);
    }

    @Test
    public void testTwoStarsWithProjectionNoFilter() {
        CQuery q = createQuery(x, aaT, nameEx  , u, aaCu,
                y, aaT, nameEx,   u, aaCu,
                y, ageEx,         v, aaCv,
                y, university_id, o, aaCo, Projection.of("o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 2);
        assertEquals(index.getStar(0).getTriples(), q.attr().getSet().subset(q.get(0)));
        IndexSubset<Triple> star1Triples = q.attr().getSet().fullSubset();
        star1Triples.remove(q.get(0));
        assertEquals(index.getStar(1).getTriples(), star1Triples);

        assertValidIndex(index, true);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(cu, co));
        assertEquals(exposedColumns(index),
                newHashSet(cu /*required by join */,
                        co /* required by projection */));
        assertEquals(exposedOuterColumns(index), singleton(co));
        assertEquals(index.getOuterProjection().size(), 1);
    }

    @Test
    public void testThreeStars() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                y, aaT, nameEx, u, aaCu,
                y, aaT, university_id, o, aaCo,
                z, aaT, university_id, o, aaCo,
                z, aaT, nameEx, v, aaCv, Projection.of("x", "v"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 3);
        assertValidIndex(index, true);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));
        checkJoinVars(index, y, z, singletonList(co), singletonList(co));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(cu, co));
        assertEquals(exposedColumns(index, z), newHashSet(co, cv));

        assertEquals(exposedColumns(index), newHashSet(
                cu, /* required by projection of x and join */
                co, /* required by join */
                cv  /* required by projection */
        ));

        assertEquals(index.getOuterProjection().size(), 2);
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv));
    }

    @Test
    public void testThreeStarsTwoIdColumns() {
        CQuery q = createQuery(x, aaT, nameEx, v, aaCvv,
                               y, aaT, nameEx, v, aaCvv,
                               y, aaT, university_id, o, aaCo,
                               z, aaT, university_id, o, aaCo,
                               z, aaT, nameEx, u, aaCu, Projection.of("x", "u"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 3);
        // the join on v is non-direct and must be re-done on SPARQL
        // thus x and y also fetch T.cu
        assertValidIndex(index, false);

        checkJoinVars(index, x, y, asList(cv1, cv2), asList(cv1, cv2));
        checkJoinVars(index, y, z, singletonList(co), singletonList(co));

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv1, cv2));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv1, cv2, co));
        assertEquals(exposedColumns(index, z), newHashSet(co, cu));

        assertEquals(exposedColumns(index), newHashSet(
                cv1, cv2, /* required by projection of x and join */
                co, /* required by join */
                cu  /* required by projection */
        ));

        assertEquals(exposedOuterColumns(index), newHashSet(cv1, cv2, cu));
    }

    @Test
    public void testExposeColumnUsedInCrossStarFilter() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                x, aaT, ageEx, v1, aaCv,
                y, aaT, nameEx, u, aaCu,
                y, aaT, ageEx, v2, aaCv, SPARQLFilter.build("?v2 > ?v1"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 2);
        assertValidIndex(index, false);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv));
        assertEquals(exposedColumns(index), newHashSet(cu, cv));

        assertEquals(index.getOuterProjection().size(), 3);
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv));
    }

    @Test
    public void testExposeOnlyColumnUsedInCrossStarFilter() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                x, aaT, ageEx, v1, aaCv,
                y, aaT, nameEx, u, aaCu,
                y, aaT, ageEx, v2, aaCv, SPARQLFilter.build("?v2 > ?v1"),
                Projection.of("v1", "v2"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertEquals(index.getStarCount(), 2);
        assertValidIndex(index, false);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(cu));

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv));
        assertEquals(exposedColumns(index), newHashSet(cu, cv));

        assertEquals(index.getOuterProjection().size(), 2);
        assertEquals(exposedOuterColumns(index), newHashSet(cv));
    }

    @Test
    public void testCreateSelectorFromLiteral() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                x, aaT, ageEx, lit(23), aaCv);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        assertEquals(index.getSelectors(0).size(), 1);
        assertEquals(index.getPendingTriples(0), singleton(q.get(0)));

        // do not expose cv, since the triple that needs it is not pending
        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index), singleton(cu));
        assertEquals(exposedOuterColumns(index), singleton(cu));

        assertEquals(index.getProjection(0).size(), 1);
        assertEquals(index.getOuterProjection().size(), 1);
    }

    @Test
    public void testCreateSelectorFromFilter() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                               x, aaT, ageEx, v, aaCv, SPARQLFilter.build("?v > 23"),
                               Projection.of("x"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index), singleton(cu));
        assertEquals(exposedOuterColumns(index), singleton(cu));

        assertEquals(index.getPendingTriples(0), emptySet());
        assertEquals(index.getPendingFilters(0), emptySet());
        assertEquals(index.getSelectors(0).size(), 1);
        assertTrue(index.getSelectors(0).stream().allMatch(FilterSelector.class::isInstance));
    }

    @Test
    public void testCreateSelectorsOnJoiningTriples() {
        SPARQLFilter regexFilter = SPARQLFilter.build("REGEX(?o, \"the.*\")");
        CQuery q = createQuery(x, aaT, ageEx, v, aaCv,
                x, aaT, isAuthorOf, o, aaCo, regexFilter,
                y, aaT, ageEx, v, aaCv, SPARQLFilter.build("?v > 23"),
                Projection.of("x", "y", "v"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, false); //u is fetched for x and y

        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);

        assertEquals(index.getPendingTriples(0), newHashSet(q.get(0), q.get(1)));
        assertEquals(index.getPendingTriples(1), emptySet());
        assertEquals(index.getPendingFilters(0), singleton(regexFilter));
        assertEquals(index.getPendingFilters(1), emptySet());
        assertEquals(index.getSelectors(0).size(), 1);
        assertEquals(index.getSelectors(1).size(), 0); // filter is placed only once
        assertTrue(index.getSelectors(0).stream().allMatch(FilterSelector.class::isInstance));

        // Projection dismisses co, but it remains included because regexFilter requires it
        assertEquals(exposedColumns(index, x), newHashSet(cu, cv, co));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv));
        assertEquals(exposedColumns(index), newHashSet(cu, cv, co));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv, co));
    }

    @Test
    public void testDoNotCreateSelectorForNonDirectMapping() {
        CQuery q = createQuery(x, aaT, ageEx,      v, aaCvv, SPARQLFilter.build("?v > 23"),
                               x, aaT, isAuthorOf, o, aaCo,
                               y, aaT, ageEx,      v, aaCvv,
                               Projection.of("x", "o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, false);

        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);

        assertEquals(index.getPendingFilters(0), singleton(SPARQLFilter.build("?v > 23")));
        assertEquals(index.getStar(1).getFilters(), emptySet());
        assertEquals(index.getPendingFilters(1), emptySet());
        assertEquals(index.getSelectors(0), emptySet());
        assertEquals(index.getSelectors(1), emptySet());

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv1, cv2, co));
        assertEquals(exposedColumns(index, y), newHashSet(cu, cv1, cv2));
        assertEquals(exposedColumns(index), newHashSet(cu, cv1, cv2, co));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv1, cv2, co));

        assertEquals(index.getProjection(0).size(), 4);
        assertEquals(index.getProjection(1).size(), 3);
        assertEquals(index.getOuterProjection().size(), 5);
    }

    @Test
    public void testThreeTableJoin() {
        CQuery q = createQuery(x, aaT, idEx,      u,       aaCu,
                               x, aaT, nameEx,    Charlie, aaCv,
                               y, aaU, author_id, u,       aaKu,
                               y, aaU, paper_id,  v,       aaKv,
                               z, aaV, idEx,      v,       aaJv,
                               z, aaV, titleEx,   o,       aaJo,
                               Projection.of("o"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        checkJoinVars(index, x, y, singletonList(cu), singletonList(ku));
        checkJoinVars(index, y, z, singletonList(kv), singletonList(jv));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(ku, kv));
        assertEquals(exposedColumns(index, z), newHashSet(jv, jo));
        assertEquals(exposedOuterColumns(index), singleton(jo));
    }

    @Test
    public void testHideJoinFromSPARQL() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCu,
                               y, aaU, nameEx, u, aaKu,
                               y, aaU, ageEx,  v, aaKv, Projection.of("v"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);
        checkJoinVars(index, x, y, singletonList(cu), singletonList(ku));

        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);
        assertEquals(index.getPendingTriples(0), emptySet());
        assertEquals(index.getPendingTriples(1), singleton(q.get(2)));

        assertEquals(exposedColumns(index, x), singleton(cu));
        assertEquals(exposedColumns(index, y), newHashSet(ku, kv));
        assertEquals(exposedOuterColumns(index), newHashSet(kv));
    }

    @Test
    public void testJoinWithNonDirectColumnIsPending() {
        CQuery q = createQuery(x, aaT, nameEx, u, aaCp, //non-direct)
                               y, aaU, nameEx, u, aaKu,
                               y, aaU, ageEx, v, aaKv, Projection.of("v"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);
        checkJoinVars(index, x, y, singletonList(cp), singletonList(ku));

        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);

        // The join must be re-done in SPARQL since cp is not direct
        assertEquals(index.getPendingTriples(0), singleton(q.get(0)));
        assertEquals(index.getPendingTriples(1), newHashSet(q.get(1), q.get(2)));

        assertEquals(exposedColumns(index, x), newHashSet(cp, cu));
        assertEquals(exposedColumns(index, y), newHashSet(ku, kv));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cp, ku, kv));
    }

    @Test
    public void testReferenceObjectJoin() {
        CQuery q = createQuery(x, aaT, knows, y, aaCvv,
                               y, aaW, ageEx, v, aaLv);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        assertEquals(index.getStarCount(), 2);
        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);

        assertEquals(index.getPendingTriples(0), singleton(q.get(0)));
        assertEquals(index.getPendingTriples(1), singleton(q.get(1)));

        checkJoinVars(index, x, y, asList(cv1, cv2), asList(lu1, lu2));

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv1, cv2));
        assertEquals(exposedColumns(index, y), newHashSet(lu1, lu2, lv));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv1, cv2, lu1, lu2, lv));
    }

    @Test
    public void testReferenceObjectJoinWithProjection() {
        CQuery q = createQuery(x, aaT, knows, y, aaCvv,
                               y, aaW, ageEx, v, aaLv, Projection.of("v"));
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        assertEquals(index.getStarCount(), 2);
        assertEquals(index.getStar(0).getCore(), x);
        assertEquals(index.getStar(1).getCore(), y);

        assertEquals(index.getPendingTriples(0), singleton(q.get(0)));
        assertEquals(index.getPendingTriples(1), singleton(q.get(1)));

        checkJoinVars(index, x, y, asList(cv1, cv2), asList(lu1, lu2));

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv1, cv2));
        assertEquals(exposedColumns(index, y), newHashSet(lu1, lu2, lv));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv1, cv2, lu1, lu2, lv));
    }

    @Test
    public void testGetColumnsForReconstructingValue() {
        CQuery q = createQuery(x, aaT, nameEx, lit("value"), aaCvv);
        StarVarIndex index = new StarVarIndex(q, selectorFactory);
        assertValidIndex(index, true);

        assertEquals(index.getPendingTriples(0), singleton(q.get(0)));
        assertEquals(index.getPendingFilters(0), emptySet());

        assertEquals(exposedColumns(index, x), newHashSet(cu, cv1, cv2));
        assertEquals(exposedOuterColumns(index), newHashSet(cu, cv1, cv2));
        assertEquals(index.getProjection(0).size(), 3);
    }
}