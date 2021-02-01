package br.ufsc.lapesd.freqel.rel.sql;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.rel.common.StarsHelper;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.context.ContextMapping;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class SqlGeneratorTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");
    private static final Column ko = new Column("U", "ko");

    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T"))
            .tag(new ColumnsTag(singletonList(cu), false)).buildAtom();
    private static final Atom atomU = Molecule.builder("U").tag(new TableTag("U"))
            .tag(new ColumnsTag(singletonList(ku), false)).buildAtom();
    private static final Atom atomCu = Molecule.builder("T").tag(ColumnsTag.direct(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("T").tag(ColumnsTag.direct(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("T").tag(ColumnsTag.direct(co)).buildAtom();
    private static final Atom atomKu = Molecule.builder("Ku").tag(ColumnsTag.direct(ku)).buildAtom();
    private static final Atom atomKv = Molecule.builder("Kv").tag(ColumnsTag.direct(kv)).buildAtom();
    private static final Atom atomKo = Molecule.builder("Ko").tag(ColumnsTag.direct(ko)).buildAtom();
    private static final Atom atomT2 = Molecule.builder("T").tag(new TableTag("T"))
                                                            .tag(ColumnsTag.direct(ku)).buildAtom();
    private static final Atom atomCu2 = Molecule.builder("Cu").tag(ColumnsTag.direct(cu))
                                                            .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCv2 = Molecule.builder("Cv").tag(ColumnsTag.direct(cv))
                                                            .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCu3 = Molecule.builder("Cu").tag(ColumnsTag.direct(cu))
                                                              .tag(ColumnsTag.direct(ku)).buildAtom();
    private static final Atom atomCv3 = Molecule.builder("Cv").tag(ColumnsTag.direct(cv))
                                                              .tag(ColumnsTag.direct(kv)).buildAtom();

    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaU = AtomAnnotation.of(atomU);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);
    private static final AtomAnnotation aaKu = AtomAnnotation.of(atomKu);
    private static final AtomAnnotation aaKv = AtomAnnotation.of(atomKv);
    private static final AtomAnnotation aaKo = AtomAnnotation.of(atomKo);
    private static final AtomAnnotation aaT2 = AtomAnnotation.of(atomT2);
    private static final AtomAnnotation aaCu2 = AtomAnnotation.of(atomCu2);
    private static final AtomAnnotation aaCu3 = AtomAnnotation.of(atomCu3);
    private static final AtomAnnotation aaCv2 = AtomAnnotation.of(atomCv2);
    private static final AtomAnnotation aaCv3 = AtomAnnotation.of(atomCv3);

    @DataProvider
    public static @Nonnull Object[][] writeSqlData() {
        StdLit bob = StdLit.fromUnescaped("bob", xsdString);

        return Stream.of(
                // simplest possible query
                // Note: star_0.cu instead of star_0.u due to lack of (SELECT ...)
                asList(createQuery(x, aaT, name, u, aaCu, Projection.of("u")),
                       "sql-mapping-1.json",
                       "SELECT star_0.cu AS $ FROM T AS star_0 ;",
                       singleton(cu), emptySet()),
                // keep x in results, its URI will be reconstructed from T.cu (already fetched)
                asList(createQuery(x, aaT, name, u, aaCu),
                        "sql-mapping-1.json",
                        "SELECT star_0.cu AS $ FROM T AS star_0 ;",
                        singleton(cu), emptySet()),
                // keep x in results but fetch T.cv, forcing T.cu inclusion
                asList(createQuery(x, aaT, age, v, aaCv),
                        "sql-mapping-1.json",
                        "SELECT star_0.@ AS $, star_0.@ AS $ " +
                                "FROM T AS star_0 ;",
                        newHashSet(cu, cv), emptySet()),
                // simple join between two stars
                asList(createQuery(x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu, Projection.of("v", "u")),
                       "sql-mapping-1.json",
                       "SELECT star_1.@ AS $, star_0.@ AS $ FROM " +
                                "T AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.@ = star_0.@);",
                       newHashSet(cu, cv), emptySet()),
                // join between two stars, first star has WHERE
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu, Projection.of("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.@ AS $, star_0.$ AS $ FROM " +
                                "(SELECT T.@ AS $ FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.@ = star_0.$);",
                        newHashSet(cu, cv), emptySet()),
                // join between three stars, first star has WHERE and second has FILTER
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu,
                                   z, aaT, name, u, aaCu,
                                   z, aaT, p1,   o, aaCo, JenaSPARQLFilter.build("?o > 23"),
                                   Projection.of("v", "u", "o")),
                        "sql-mapping-1.json",
                        "SELECT star_2.$ AS $, star_1.@ AS $, star_0.$ AS $ FROM " +
                                "(SELECT T.@ AS $ FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.@ = star_0.$) " +
                                "INNER JOIN (SELECT T.@ AS $, T.@ AS $ " +
                                        "FROM T WHERE (T.@ > 23) ) AS star_2 " +
                                        "ON (star_2.$ = star_1.@);",
                        newHashSet(cv, cu, co), emptySet()),
                // same as above, but keep subjects, introduce new column for x
                // new column is required since existing u -> T.cu comes from another star
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu,
                                   z, aaT, name, u, aaCu,
                                   z, aaT, p1,   o, aaCo, JenaSPARQLFilter.build("?o > 23")),
                        "sql-mapping-1.json",
                        "SELECT star_0.$ AS $, " +
                                        "star_2.$ AS $, star_1.@ AS $, star_0.$ AS $ FROM " +
                                "(SELECT T.@ AS $, T.@ AS $ " +
                                        "FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.@ = star_0.$) " +
                                "INNER JOIN (SELECT T.@ AS $, T.@ AS $ " +
                                "FROM T WHERE (T.@ > 23) ) AS star_2 " +
                                "ON (star_2.$ = star_1.@);",
                        newHashSet(cu, cv, co), emptySet()),
                // single star with WHERE clause and pending filter. x projection has no effect
                asList(createQuery(x, aaT, name, u, aaCu, JenaSPARQLFilter.build("REGEX(?u, \"b.*\")"),
                                   x, aaT, age,  v, aaCv, JenaSPARQLFilter.build("?v < 23")),
                       "sql-mapping-1.json",
                       "SELECT star_0.$ AS $, star_0.$ AS $ " +
                               "FROM (SELECT T.@ AS $, T.@ AS $ FROM T WHERE (T.@ < 23)) " +
                                        "AS star_0 ;",
                       newHashSet(cu, cv), singleton(JenaSPARQLFilter.build("REGEX(?u, \"b.*\")"))),
                // tolerate ColumnTag on Subject and TableTag on object
                asList(createQuery(x, aaT2, name, u, aaCu2, Projection.of("u")),
                        "sql-mapping-1.json",
                        "SELECT star_0.@ AS $ FROM T AS star_0 ;",
                        singleton(cu), emptySet()),
                // tolerate ColumnTag on subject and unrelated ColumnTag on object
                asList(createQuery(x, aaT2, name, u, aaCu3, Projection.of("u")),
                        "sql-mapping-1.json",
                        "SELECT star_0.@ AS $ FROM T AS star_0 ;",
                        singleton(cu), emptySet()),
                // join between two stars from different tables
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaU, age,  v, aaKv,
                                   y, aaU, name, u, aaKu, Projection.of("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.@ AS $, star_0.$ AS $ FROM " +
                                "(SELECT T.@ AS $ FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN U AS star_1 ON (star_1.@ = star_0.$);",
                        newHashSet(cv, ku), emptySet()),
                // join between two stars with bogus ColumnTags on subjects
                asList(createQuery(x, aaT2, name, bob, aaCu2,
                                   x, aaT2, age,  v, aaCv2, //cv3 is really ambiguous
                                   y, aaU, age,  v, aaKv,
                                   y, aaU, name, u, aaKu, Projection.of("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.@ AS $, star_0.$ AS $ FROM " +
                                "(SELECT T.@ AS $ FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN U AS star_1 ON (star_1.@ = star_0.$);",
                        newHashSet(cv, ku), emptySet()),
                // join between three stars, with WHERE, FILTER, two tables and extra annotations
                asList(createQuery(x, aaT, name, bob, aaCu3, // not ambiguous as bob is not shared
                                   x, aaT, age,  v, aaCv3,
                                   y, aaT, age,  v, aaCv3,
                                   y, aaT, name, u, aaCu2,
                                   z, aaU, name, u, aaKu,
                                   z, aaU, p1,   o, aaKo, JenaSPARQLFilter.build("?o > 23")),
                        "sql-mapping-1.json",
                        "SELECT star_[012].$ AS $, star_[012].$ AS $, star_[012].$ AS $, star_[012].@ AS $, " +
                                "star_[012].$ AS $ FROM " +
                                "(SELECT T.@ AS $, T.@ AS $ " +
                                        "FROM T WHERE T.@ = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.@ = star_0.$) " +
                                "INNER JOIN (SELECT U.@ AS $, U.@ AS $ " +
                                        "FROM U WHERE (U.@ > 23) ) AS star_2 " +
                                        "ON (star_2.$ = star_1.@);",
                        newHashSet(cu, cv, ku, ko), emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "writeSqlData")
    public void testWriteSql(@Nonnull CQuery query, @Nonnull String mappingPath,
                             @Nonnull String expectedSql, @Nonnull Set<Column> expectedColumns,
                             @Nonnull Set<SPARQLFilter> pendingFilters) throws Exception {
        ContextMapping mapping = ContextMapping.parse(
                DictTree.load().fromResourceList(getClass(), mappingPath));
        SqlGenerator generator = new SqlGenerator(mapping);
        RelationalRewriting sql = generator.transform(query);
        assertEquals(sql.isDistinct(),
                     ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null);
        Set<Column> actualColumns = sql.getVars().stream().map(sql.getIndex()::getColumn)
                                                          .collect(Collectors.toSet());
        assertEquals(actualColumns, expectedColumns);

        IndexSet<SPARQLFilter> filters = StarsHelper.getFilters(query);
        assertTrue(filters.containsAll(pendingFilters));
        assertEquals(sql.getPendingFilters(), pendingFilters);
        assertEquals(sql.getDoneFilters(), filters.fullSubset().createMinus(pendingFilters));

        String rxString = expectedSql.replaceAll(" +", "\\\\s*")
                                     .replaceAll("@", "[ck][uvo]")
                                     .replaceAll("\\$", "vi?\\\\d+")
                                     .replaceAll("([()])", "\\\\s*\\\\$1\\\\s*");
        assertTrue(Pattern.compile(rxString).matcher(sql.getRelationalQuery()).matches(),
                "\n\""+sql.getRelationalQuery()+"\" does not match \n\""+expectedSql+"\"");
    }
}