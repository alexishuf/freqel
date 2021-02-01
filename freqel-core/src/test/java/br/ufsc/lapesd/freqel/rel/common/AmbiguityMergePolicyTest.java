package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class AmbiguityMergePolicyTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");
    private static final Column ko = new Column("U", "ko");

    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T")).buildAtom();
    private static final Atom atomU = Molecule.builder("U").tag(new TableTag("U")).buildAtom();
    private static final Atom atomCu = Molecule.builder("T").tag(ColumnsTag.direct(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("T").tag(ColumnsTag.direct(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("T").tag(ColumnsTag.direct(co)).buildAtom();
    private static final Atom atomKu = Molecule.builder("Ku").tag(ColumnsTag.direct(ku)).buildAtom();
    private static final Atom atomKo = Molecule.builder("Ko").tag(ColumnsTag.direct(ko)).buildAtom();

    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaU = AtomAnnotation.of(atomU);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);
    private static final AtomAnnotation aaKu = AtomAnnotation.of(atomKu);
    private static final AtomAnnotation aaKo = AtomAnnotation.of(atomKo);

    @DataProvider
    public static Object[][] testData() {
        return Stream.of(
                // column ambiguity
                asList(createQuery(x, aaT, p1, u, aaCu),
                       createQuery(x, aaT, p1, u, aaCo), false),
                // table and column ambiguity
                asList(createQuery(x, aaT, p1, u, aaCu),
                       createQuery(x, aaU, p1, u, aaKu), false),
                // table ambiguity
                asList(createQuery(x, aaT, p1, u, aaCu),
                       createQuery(x, aaU, p1, u, aaCu), false),
                // valid merge (different subjects, same object)
                asList(createQuery(x, aaT, p1, u, aaCu),
                       createQuery(y, aaT, p1, u, aaCu), true),
                // valid merge (different subjects, same object) table disambiguates column
                asList(createQuery(x, aaT, p1, u, aaCu),
                       createQuery(y, aaU, p1, u, aaKu), true),
                // valid merge: unrelated queries
                asList(createQuery(x, aaT, p1, u, aaCu,
                                   x, aaT, p2, v, aaCv),
                       createQuery(y, aaU, p3, o, aaKo), true),
                // valid merge: no column ambiguity on u
                asList(createQuery(x, aaT, p1, u, aaCu,
                                   x, aaT, p2, v, aaCv),
                       createQuery(y, aaU, p3, u, aaKu), true),
                // valid merge: no column ambiguity on u
                asList(createQuery(x, aaT, p1, u, aaCu,
                                   x, aaT, p2, v, aaCv),
                       createQuery(y, aaT, p3, u, aaCo), false)
                ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull CQuery a, @Nonnull CQuery b, boolean expected) {
        AmbiguityMergePolicy policy = new AmbiguityMergePolicy();
        assertEquals(policy.canMerge(a, b), expected);
        assertEquals(policy.canMerge(b, a), expected);
    }
}