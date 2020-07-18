package br.ufsc.lapesd.riefederator.rel.mappings.r2rml;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.tags.ValueTag;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.PostRelationalTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = {"fast"})
public class RRMappingTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] moleculeData() {
        return Stream.of(
                asList("r2rml-1.ttl",
                        Molecule.builder(Professor.getURI())
                                .out(type, Molecule.builder(Professor.getURI()+"-type")
                                                .tag(new ValueTag(Professor))
                                                .tag(PostRelationalTag.INSTANCE).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(p1, Molecule.builder(p1.getURI())
                                                .tag(ColumnsTag.createDirect("T1", "cB"))
                                                .buildAtom(),
                                        ColumnsTag.createDirect("T1", "cB"))
                                .out(p2, Molecule.builder(p2.getURI())
                                                .tag(ColumnsTag.createDirect("T1", "cC")).buildAtom(),
                                        ColumnsTag.createDirect("T1", "cC"))
                                .out(p3, Molecule.builder(p3.getURI())
                                                .tag(new ColumnsTag(asList(new Column("T1", "cD"),
                                                                          new Column("T1", "cE"))))
                                                .buildAtom(),
                                        new ColumnsTag(asList(new Column("T1", "cD"),
                                                             new Column("T1", "cE"))))
                                .tag(new TableTag("T1"))
                                .tag(ColumnsTag.createNonDirect("T1", "cA"))
                                .exclusive().closed().build()
                ),
                asList("r2rml-2.ttl",
                        Molecule.builder(University.getURI())
                                .out(type, Molecule.builder(University.getURI()+"-type")
                                                .tag(ValueTag.of(University))
                                                .tag(PostRelationalTag.INSTANCE).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(nameEx, Molecule.builder(nameEx.getURI()+"-2")
                                                .tag(ColumnsTag.createDirect("University", "nm"))
                                                .buildAtom(),
                                        ColumnsTag.createDirect("University", "nm"))
                                .tag(new TableTag("University"))
                                .tag(ColumnsTag.createNonDirect("Professor", "uni_id")) // from the referencing object map
                                .tag(ColumnsTag.createNonDirect("University", "id"))
                                .exclusive().closed()
                                .startNewCore(Professor.getURI())
                                .out(type, Molecule.builder(Professor.getURI()+"-type")
                                                .tag(ValueTag.of(Professor))
                                                .tag(PostRelationalTag.INSTANCE).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(nameEx, Molecule.builder(nameEx.getURI()+"-1")
                                                .tag(ColumnsTag.createDirect("Professor", "nm"))
                                                .buildAtom(),
                                        ColumnsTag.createDirect("Professor", "nm"))
                                .out(university, University.getURI(),
                                        ColumnsTag.createNonDirect("Professor", "uni_id"))
                                .tag(new TableTag("Professor"))
                                .tag(ColumnsTag.createNonDirect("Professor", "id"))
                                .exclusive().closed().build()
                )
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "moleculeData")
    public void testMolecule(@Nonnull String resourcePath,
                             @Nonnull Molecule expected) throws IOException {
        RRMapping mapping;
        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            assertNotNull(in);
            mapping = RRMapping.builder().name(resourcePath).load(in);
        }

        Molecule actual = mapping.createMolecule();
        assertEquals(actual, expected);
    }

}