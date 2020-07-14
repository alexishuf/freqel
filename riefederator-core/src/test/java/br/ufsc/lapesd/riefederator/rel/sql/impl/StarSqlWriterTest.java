package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.impl.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.rel.sql.DefaultSqlTermWriter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class StarSqlWriterTest implements TestContext {
    private static final IndexedSubset<SPARQLFilter> EMPTY_FILTERS =
            IndexedSet.from(new ArrayList<SPARQLFilter>()).emptySubset();

    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");
    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T")).buildAtom();
    private static final Atom atomCu = Molecule.builder("Cu").tag(new ColumnTag(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("Cv").tag(new ColumnTag(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("Co").tag(new ColumnTag(co)).buildAtom();
    private static final Atom atomKu = Molecule.builder("Ku").tag(new ColumnTag(ku)).buildAtom();
    private static final Atom atomT2 = Molecule.builder("T").tag(new TableTag("T"))
                                                            .tag(new ColumnTag(ku)).buildAtom();
    private static final Atom atomCu2 = Molecule.builder("Cu").tag(new ColumnTag(cu))
                                                              .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCv2 = Molecule.builder("Cv").tag(new ColumnTag(cv))
                                                              .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCu3 = Molecule.builder("Cu").tag(new ColumnTag(cu))
                                                              .tag(new ColumnTag(ku)).buildAtom();
    private static final Atom atomCv3 = Molecule.builder("Cv").tag(new ColumnTag(cv))
                                                              .tag(new ColumnTag(kv)).buildAtom();
    private static final Lit i23 = StdLit.fromUnescaped("23", xsdInt);
    private static final Lit i24 = StdLit.fromUnescaped("24", xsdInt);

    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);
    private static final AtomAnnotation aaKu = AtomAnnotation.of(atomKu);
    private static final AtomAnnotation aaT2 = AtomAnnotation.of(atomT2);
    private static final AtomAnnotation aaCu2 = AtomAnnotation.of(atomCu2);
    private static final AtomAnnotation aaCu3 = AtomAnnotation.of(atomCu3);
    private static final AtomAnnotation aaCv2 = AtomAnnotation.of(atomCv2);
    private static final AtomAnnotation aaCv3 = AtomAnnotation.of(atomCv3);

    @Test
    public void testToSelector() {
        StarSqlWriter w = new StarSqlWriter(DefaultSqlTermWriter.INSTANCE);
        StdBlank blank = new StdBlank();
        CQuery query = createQuery(
                x, AtomAnnotation.of(atomT), name, u, AtomAnnotation.of(atomCu),
                x, AtomAnnotation.of(atomT), age, lit(23), AtomAnnotation.of(atomCv),
                x, AtomAnnotation.of(atomT), knows, blank, AtomAnnotation.of(atomCo));
        StarSubQuery star = new StarSubQuery(query.getSet().fullSubset(),
                                             newHashSet("x", "u"), EMPTY_FILTERS, query);
        StarVarIndex varIndex = new StarVarIndex(query, singletonList(star));
        w.setUp(varIndex, 0);

        SqlSelector assignSelector = w.toSelector(query.getList().get(0));
        assertNotNull(assignSelector);
        assertFalse(assignSelector.hasSqlCondition());
        assertEquals(assignSelector.getColumns(), singletonList(cu));
        assertEquals(assignSelector.getSparqlTerms(), singletonList(u));
        assertEquals(assignSelector.getSparqlVars(), singleton("u"));
        assertEquals(((AssignSqlSelector)assignSelector).getSparqlVar(), "u");
        assertEquals(((AssignSqlSelector)assignSelector).getColumn(), cu);

        SqlSelector eqSelector = w.toSelector(query.getList().get(1));
        assertNotNull(eqSelector);
        assertTrue(eqSelector.hasSqlCondition());
        assertTrue(eqSelector.getSqlCondition().matches("\\(?T.cv = 23\\)?"));
        assertEquals(eqSelector.getColumns(), singletonList(cv));
        assertEquals(eqSelector.getSparqlTerms(), singletonList(lit(23)));
        assertEquals(eqSelector.getSparqlVars(), emptySet());

        // alien triple is not annotated
        expectThrows(IllegalArgumentException.class, () -> w.toSelector(new Triple(x, ageEx, o)));

        // blank node translates into IS NOT NULL
        SqlSelector nnSelector = w.toSelector(query.getList().get(2));
        assertNotNull(nnSelector);
        assertEquals(nnSelector.getColumns(), singletonList(co));
        assertEquals(nnSelector.getSparqlTerms(), singletonList(blank));
        assertEquals(nnSelector.getSparqlVars(), emptySet());
        assertTrue(nnSelector.hasSqlCondition());
        assertTrue(nnSelector.getSqlCondition().matches("\\(?T.co IS NOT NULL\\)?"));
    }

    @DataProvider
    public static @Nonnull Object[][] filterData() {
        return Stream.of(
                asList(SPARQLFilter.build("regex(?u, \"a.*\")"), null),
                asList(SPARQLFilter.build("?v > 23"), "(T.cv > 23)"),
                asList(SPARQLFilter.build("?v > 23 && ?v < 30"), "((T.cv > 23) AND (T.cv < 30))"),
                asList(SPARQLFilter.build("?v > 23 && (?v < 30 || ?o > 60)"),
                        "((T.cv > 23) AND ((T.cv < 30) OR (T.co > 60)))")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "filterData")
    public void testFilterSelector(@Nonnull SPARQLFilter filter, @Nullable String expected) {
        StarSqlWriter w = new StarSqlWriter(DefaultSqlTermWriter.INSTANCE);
        CQuery query = createQuery(
                x, AtomAnnotation.of(atomT), name, u, AtomAnnotation.of(atomCu),
                x, AtomAnnotation.of(atomT), age, v, AtomAnnotation.of(atomCv),
                x, AtomAnnotation.of(atomT), ageEx, o, AtomAnnotation.of(atomCo));
        StarSubQuery star = new StarSubQuery(query.getSet().fullSubset(),
                newHashSet("x", "u", "v", "o"), EMPTY_FILTERS, query);
        StarVarIndex starVarIndex = new StarVarIndex(query, singletonList(star));
        w.setUp(starVarIndex, 0);
        SqlSelector selector = w.toSelector(filter);
        if (expected == null) {
            assertNull(selector);
        } else {
            assertNotNull(selector);
            assertEquals(selector.getSparqlVars(), filter.getVarTermNames());
            assertEquals(new HashSet<>(selector.getSparqlTerms()), filter.getVarTerms());

            Set<Column> expectedColumns = filter.getVarTermNames().stream()
                    .map(n -> new Column("T", "c" + n)).collect(toSet());
            assertEquals(new HashSet<>(selector.getColumns()), expectedColumns);
            assertTrue(selector.hasSqlCondition());
            assertEquals(selector.getSqlCondition(), expected);
        }
    }
    @DataProvider
    public static @Nonnull Object[][] writeSqlData() {
        return Stream.of(
                asList(createQuery(x, aaT, name, u, aaCu, Projection.advised("u")),
                                  "T AS star_0", null, null),
                asList(createQuery(x, aaT, name, u, aaCu, x, aaT, age, i23, aaCv,
                                   Projection.required("u")),
                       "(SELECT T.cu AS u FROM T WHERE T.cv = 23) AS star_0",
                       null, null),
                asList(createQuery(x, aaT, name, u, aaCu,
                                   x, aaT, age, i23, aaCv,
                                   x, aaT, ageEx, i24, aaCo,
                                   Projection.required("u")),
                       "(SELECT T.cu AS u FROM T WHERE T.cv = 23 AND T.co = 24) AS star_0",
                       null, null),
                asList(createQuery(x, aaT, age, v, aaCv),
                                   "T AS star_0", null, null),
                asList(createQuery(x, aaT, age, v, aaCv, SPARQLFilter.build("?v < 23")),
                                   "(SELECT T.cv AS v, T.cu AS _riefederator_id_col_0 " +
                                   "FROM T WHERE (T.cv < 23)) AS star_0", null, null),
                // first case with multiple AtomAnnotations on subject
                asList(createQuery(x, aaT, aaKu, name, u, aaCu, Projection.advised("u")),
                        "T AS star_0", null, null),
                // tolerate TableTag on subject atom
                asList(createQuery(x, aaT, aaKu, name, u, aaCu2, Projection.advised("u")),
                        "T AS star_0", null, null),
                // tolerate two ColumnTags on object atom
                asList(createQuery(x, aaT, name, u, aaCu3, Projection.advised("u")),
                        "T AS star_0", null, null),
                // tolerate ColumnTag on subject atom
                asList(createQuery(x, aaT2, name, u, aaCu3, Projection.advised("u")),
                        "T AS star_0", null, null),
                //tolerate table tag on object and column tag on subject
                asList(createQuery(x, aaT2, name,   u,  aaCu2,
                                   x, aaT2, age,   i23, aaCv2,
                                   Projection.required("u")),
                        "(SELECT T.cu AS u FROM T WHERE T.cv = 23) AS star_0",
                        null, null),
                // tolerate unrelated column tag on object
                asList(createQuery(x, aaT2, name,   u,  aaCu3,
                                   x, aaT2, age,   i23, aaCv3,
                                   Projection.required("u")),
                        "(SELECT T.cu AS u FROM T WHERE T.cv = 23) AS star_0",
                        null, null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "writeSqlData")
    public void testWriteSql(@Nonnull CQuery query, @Nonnull String expected,
                             @Nullable Set<SPARQLFilter> pendingFilters,
                             @Nullable Set<SPARQLFilter> expectedPendingFilters) {
        ContextMapping mapping = ContextMapping.builder().beginTable("T")
                .addIdColumn("cu")
                .fallbackPrefix(EX)
                .instancePrefix(EX + "inst/").endTable().build();
        assertEquals(pendingFilters == null, expectedPendingFilters == null);
        List<StarSubQuery> stars = StarsHelper.findStars(query);
        assertEquals(stars.size(), 1);
        StarVarIndex index = new StarVarIndex(query, stars, mapping, false);

        StarSqlWriter w = new StarSqlWriter(DefaultSqlTermWriter.INSTANCE);
        String actual = w.write(index, 0, pendingFilters);
        if (pendingFilters != null)
            assertEquals(pendingFilters, expectedPendingFilters);

        String rxString = expected.replaceAll(" +", "\\\\s*").replaceAll("([()])", "\\\\s*\\\\$1\\\\s*");
        assertTrue(Pattern.compile(rxString).matcher(actual).matches(),
                   "\n\""+actual+"\" does not match \n\""+expected+"\"");
    }

}