package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.context.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class StarSqlWriterTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");
    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T"))
            .tag(ColumnsTag.createNonDirect("T", "cu")).buildAtom();
    private static final Atom atomCu = Molecule.builder("Cu").tag(ColumnsTag.createDirect(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("Cv").tag(ColumnsTag.createDirect(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("Co").tag(ColumnsTag.createDirect(co)).buildAtom();
    private static final Atom atomKu = Molecule.builder("Ku").tag(ColumnsTag.createDirect(ku)).buildAtom();
    private static final Atom atomT2 = Molecule.builder("T").tag(new TableTag("T"))
                                                            .tag(ColumnsTag.createDirect(ku)).buildAtom();
    private static final Atom atomCu2 = Molecule.builder("Cu").tag(ColumnsTag.createDirect(cu))
                                                              .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCv2 = Molecule.builder("Cv").tag(ColumnsTag.createDirect(cv))
                                                              .tag(new TableTag("U")).buildAtom();
    private static final Atom atomCu3 = Molecule.builder("Cu").tag(ColumnsTag.createDirect(cu))
                                                              .tag(ColumnsTag.createDirect(ku)).buildAtom();
    private static final Atom atomCv3 = Molecule.builder("Cv").tag(ColumnsTag.createDirect(cv))
                                                              .tag(ColumnsTag.createDirect(kv)).buildAtom();
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


    @DataProvider
    public static @Nonnull Object[][] writeSqlData() {
        return Stream.of(
                asList(createQuery(x, aaT, name, u, aaCu, Projection.advised("u")),
                                  "T AS star_0"),
                asList(createQuery(x, aaT, name, u, aaCu, x, aaT, age, i23, aaCv,
                                   Projection.required("u")),
                       "(SELECT T.cu AS $ FROM T WHERE T.cv = 23) AS star_0"),
                asList(createQuery(x, aaT, name, u, aaCu,
                                   x, aaT, age, i23, aaCv,
                                   x, aaT, ageEx, i24, aaCo,
                                   Projection.required("u")),
                       "(SELECT T.cu AS $ FROM T WHERE T.cv = 23 AND T.co = 24) AS star_0"),
                asList(createQuery(x, aaT, age, v, aaCv),
                                   "T AS star_0"),
                asList(createQuery(x, aaT, age, v, aaCv, SPARQLFilter.build("?v < 23")),
                                   "(SELECT T.cv AS $, T.cu AS $ " +
                                   "FROM T WHERE (T.cv < 23)) AS star_0"),
                // first case with multiple AtomAnnotations on subject
                asList(createQuery(x, aaT, aaKu, name, u, aaCu, Projection.advised("u")),
                        "T AS star_0"),
                // tolerate TableTag on subject atom
                asList(createQuery(x, aaT, aaKu, name, u, aaCu2, Projection.advised("u")),
                        "T AS star_0"),
                // tolerate two ColumnTags on object atom
                asList(createQuery(x, aaT, name, u, aaCu3, Projection.advised("u")),
                        "T AS star_0"),
                // tolerate ColumnTag on subject atom
                asList(createQuery(x, aaT2, name, u, aaCu3, Projection.advised("u")),
                        "T AS star_0"),
                //tolerate table tag on object and column tag on subject
                asList(createQuery(x, aaT2, name,   u,  aaCu2,
                                   x, aaT2, age,   i23, aaCv2,
                                   Projection.required("u")),
                        "(SELECT T.cu AS $ FROM T WHERE T.cv = 23) AS star_0"),
                // tolerate unrelated column tag on object
                asList(createQuery(x, aaT2, name,   u,  aaCu3,
                                   x, aaT2, age,   i23, aaCv3,
                                   Projection.required("u")),
                        "(SELECT T.cu AS $ FROM T WHERE T.cv = 23) AS star_0")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "writeSqlData")
    public void testWriteSql(@Nonnull CQuery query, @Nonnull String expected) {
        ContextMapping mapping = ContextMapping.builder().beginTable("T")
                .addIdColumn("cu")
                .fallbackPrefix(EX)
                .instancePrefix(EX + "inst/").endTable().build();
        SqlSelectorFactory selectorFactory = new SqlSelectorFactory(DefaultSqlTermWriter.INSTANCE);
        StarVarIndex index = new StarVarIndex(query, selectorFactory);

        StarSqlWriter w = new StarSqlWriter();
        String actual = w.write(index, 0);

        String rxString = expected.replaceAll(" +", "\\\\s*")
                .replaceAll("\\$", "vi?\\\\d+")
                .replaceAll("([()])", "\\\\s*\\\\$1\\\\s*");
        assertTrue(Pattern.compile(rxString).matcher(actual).matches(),
                   "\n\""+actual+"\" does not match \n\""+expected+"\"");
    }

}