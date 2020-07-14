package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.impl.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
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

    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T")).buildAtom();
    private static final Atom atomU = Molecule.builder("U").tag(new TableTag("U")).buildAtom();
    private static final Atom atomCu = Molecule.builder("T").tag(new ColumnTag(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("T").tag(new ColumnTag(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("T").tag(new ColumnTag(co)).buildAtom();
    private static final Atom atomKu = Molecule.builder("Ku").tag(new ColumnTag(ku)).buildAtom();
    private static final Atom atomKv = Molecule.builder("Kv").tag(new ColumnTag(kv)).buildAtom();
    private static final Atom atomKo = Molecule.builder("Ko").tag(new ColumnTag(ko)).buildAtom();
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
                asList(createQuery(x, aaT, name, u, aaCu, Projection.advised("u")),
                       "sql-mapping-1.json",
                       "SELECT star_0.cu AS u FROM T AS star_0 ;",
                       singleton("u"), emptySet()),
                // keep x in results, its URI will be reconstructed from T.cu (already fetched)
                asList(createQuery(x, aaT, name, u, aaCu),
                        "sql-mapping-1.json",
                        "SELECT star_0.cu AS u FROM T AS star_0 ;",
                        singleton("u"), emptySet()),
                // keep x in results but fetch T.cv, forcing T.cu inclusion
                asList(createQuery(x, aaT, age, v, aaCv),
                        "sql-mapping-1.json",
                        "SELECT star_0.cu AS _riefederator_id_col_0, star_0.cv AS v " +
                                "FROM T AS star_0 ;",
                        newHashSet("v", "_riefederator_id_col_0"), emptySet()),
                // simple join between two stars
                asList(createQuery(x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu, Projection.required("v", "u")),
                       "sql-mapping-1.json",
                       "SELECT star_1.cu AS u, star_0.cv AS v FROM " +
                                "T AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.cv = star_0.cv);",
                       newHashSet("v", "u"), emptySet()),
                // join between two stars, first star has WHERE
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu, Projection.required("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.cu AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.cv = star_0.v);",
                        newHashSet("v", "u"), emptySet()),
                // join between three stars, first star has WHERE and second has FILTER
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu,
                                   z, aaT, name, u, aaCu,
                                   z, aaT, p1,   o, aaCo, SPARQLFilter.build("?o > 23"),
                                   Projection.required("v", "u", "o")),
                        "sql-mapping-1.json",
                        "SELECT star_2.o AS o, star_1.cu AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.cv = star_0.v) " +
                                "INNER JOIN (SELECT T.cu AS u, T.co AS o " +
                                        "FROM T WHERE (T.co > 23) ) AS star_2 " +
                                        "ON (star_2.u = star_1.cu);",
                        newHashSet("v", "u", "o"), emptySet()),
                // same as above, but keep subjects, introduce new column for x
                // new column is required since existing u -> T.cu comes from another star
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaT, age,  v, aaCv,
                                   y, aaT, name, u, aaCu,
                                   z, aaT, name, u, aaCu,
                                   z, aaT, p1,   o, aaCo, SPARQLFilter.build("?o > 23")),
                        "sql-mapping-1.json",
                        "SELECT star_0._riefederator_id_col_0 AS _riefederator_id_col_0, " +
                                        "star_2.o AS o, star_1.cu AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v, T.cu AS _riefederator_id_col_0 " +
                                        "FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.cv = star_0.v) " +
                                "INNER JOIN (SELECT T.cu AS u, T.co AS o " +
                                "FROM T WHERE (T.co > 23) ) AS star_2 " +
                                "ON (star_2.u = star_1.cu);",
                        newHashSet("_riefederator_id_col_0", "v", "u", "o"), emptySet()),
                // single star with WHERE clause and pending filter. x projection has no effect
                asList(createQuery(x, aaT, name, u, aaCu, SPARQLFilter.build("REGEX(?u, \"b.*\")"),
                                   x, aaT, age,  v, aaCv, SPARQLFilter.build("?v < 23")),
                       "sql-mapping-1.json",
                       "SELECT star_0.u AS u, star_0.v AS v " +
                               "FROM (SELECT T.cu AS u, T.cv AS v FROM T WHERE (T.cv < 23)) " +
                                        "AS star_0 ;",
                       newHashSet("u", "v"), singleton(SPARQLFilter.build("REGEX(?u, \"b.*\")"))),
                // tolerate ColumnTag on Subject and TableTag on object
                asList(createQuery(x, aaT2, name, u, aaCu2, Projection.advised("u")),
                        "sql-mapping-1.json",
                        "SELECT star_0.cu AS u FROM T AS star_0 ;",
                        singleton("u"), emptySet()),
                // tolerate ColumnTag on subject and unrelated ColumnTag on object
                asList(createQuery(x, aaT2, name, u, aaCu3, Projection.advised("u")),
                        "sql-mapping-1.json",
                        "SELECT star_0.cu AS u FROM T AS star_0 ;",
                        singleton("u"), emptySet()),
                // join between two stars from different tables
                asList(createQuery(x, aaT, name, bob, aaCu,
                                   x, aaT, age,  v, aaCv,
                                   y, aaU, age,  v, aaKv,
                                   y, aaU, name, u, aaKu, Projection.required("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.ku AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN U AS star_1 ON (star_1.kv = star_0.v);",
                        newHashSet("v", "u"), emptySet()),
                // join between two stars with bogus ColumnTags on subjects
                asList(createQuery(x, aaT2, name, bob, aaCu2,
                                   x, aaT2, age,  v, aaCv2, //cv3 is really ambiguous
                                   y, aaU, age,  v, aaKv,
                                   y, aaU, name, u, aaKu, Projection.required("v", "u")),
                        "sql-mapping-1.json",
                        "SELECT star_1.ku AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN U AS star_1 ON (star_1.kv = star_0.v);",
                        newHashSet("v", "u"), emptySet()),
                // join between three stars, with WHERE, FILTER, two tables and extra annotations
                asList(createQuery(x, aaT, name, bob, aaCu3, // not ambiguous as bob is not shared
                                   x, aaT, age,  v, aaCv3,
                                   y, aaT, age,  v, aaCv3,
                                   y, aaT, name, u, aaCu2,
                                   z, aaU, name, u, aaKu,
                                   z, aaU, p1,   o, aaKo, SPARQLFilter.build("?o > 23")),
                        "sql-mapping-1.json",
                        "SELECT star_0._riefederator_id_col_0 AS _riefederator_id_col_0, " +
                                        "star_2.o AS o, star_1.cu AS u, star_0.v AS v FROM " +
                                "(SELECT T.cv AS v, T.cu AS _riefederator_id_col_0 " +
                                        "FROM T WHERE T.cu = 'bob' ) AS star_0 " +
                                "INNER JOIN T AS star_1 ON (star_1.cv = star_0.v) " +
                                "INNER JOIN (SELECT U.ku AS u, U.ko AS o " +
                                        "FROM U WHERE (U.ko > 23) ) AS star_2 " +
                                        "ON (star_2.u = star_1.cu);",
                        newHashSet("_riefederator_id_col_0", "v", "u", "o"), emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "writeSqlData")
    public void testWriteSql(@Nonnull CQuery query, @Nonnull String mappingPath,
                             @Nonnull String expectedSql, @Nonnull Set<String> sqlVars,
                             @Nonnull Set<SPARQLFilter> pendingFilters) throws Exception {
        ContextMapping mapping = ContextMapping.parse(
                DictTree.load().fromResourceList(getClass(), mappingPath));
        SqlGenerator generator = new SqlGenerator(mapping);
        SqlRewriting sql = generator.transform(query);
        assertEquals(sql.isDistinct(),
                     ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null);
        assertEquals(sql.getVars(), sqlVars);

        IndexedSet<SPARQLFilter> filters = StarsHelper.getFilters(query);
        assertTrue(filters.containsAll(pendingFilters));
        assertEquals(sql.getPendingFilters(), pendingFilters);
        assertEquals(sql.getDoneFilters(), filters.fullSubset().createDifference(pendingFilters));

        String rxString = expectedSql.replaceAll(" +", "\\\\s*")
                                     .replaceAll("([()])", "\\\\s*\\\\$1\\\\s*");
        assertTrue(Pattern.compile(rxString).matcher(sql.getSql()).matches(),
                "\n\""+sql.getSql()+"\" does not match \n\""+expectedSql+"\"");
    }
}