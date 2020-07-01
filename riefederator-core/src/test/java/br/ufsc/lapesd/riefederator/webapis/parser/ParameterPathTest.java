package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.DictTree;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ParameterPathTest {

    private DictTree tree;
    private Molecule molecule;

    private @Nullable Term property2term(@Nullable String property) {
        return property == null ? null : new StdPlain(property);
    }

    @BeforeMethod
    public void setUp() throws IOException {
        String resourcePath = "x-path-tests.yaml";
        tree = DictTree.load().fromResource(ParameterPathTest.class, resourcePath);

        Term x = property2term("x");
        Term y = property2term("y");
        Term date = property2term("date");
        molecule = Molecule.builder("Core")
                .out(x, Molecule.builder("X").out(y, new Atom("Y")).buildAtom())
                .in(y, Molecule.builder("Yin").in(x, new Atom("Xin")).buildAtom())
                .out(date, new Atom("Date"))
                .build();
    }

    @Test
    public void testSingleStep() {
        DictTree dict = tree.getMapNN("single-step");
        ParameterPath parameterPath = ParameterPath.parse(dict, molecule, this::property2term);
        assertNotNull(parameterPath);
        assertEquals(parameterPath.getPath(), singletonList(property2term("x")));
        assertEquals(parameterPath.getAtom().getName(), "X");
        assertEquals(parameterPath.getAtomFilters(), emptySet());
        assertFalse(parameterPath.isIn());
        assertFalse(parameterPath.isMissingInResult());
    }

    @Test
    public void testTwoStep() {
        DictTree dict = tree.getMapNN("two-step");
        ParameterPath parameterPath = ParameterPath.parse(dict, molecule, this::property2term);
        assertNotNull(parameterPath);
        assertEquals(parameterPath.getPath(), asList(property2term("x"), property2term("y")));
        assertEquals(parameterPath.getAtom().getName(), "Y");
        assertEquals(parameterPath.getAtomFilters(), emptySet());
        assertFalse(parameterPath.isIn());
        assertFalse(parameterPath.isMissingInResult());
    }

    @Test
    public void testMissingIn() {
        DictTree dict = tree.getMapNN("missing-in");
        ParameterPath parameterPath = ParameterPath.parse(dict, molecule, this::property2term);
        assertNotNull(parameterPath);
        assertEquals(parameterPath.getPath(), asList(property2term("x"), property2term("y")));
        assertEquals(parameterPath.getAtom().getName(), "Xin");
        assertEquals(parameterPath.getAtomFilters(), emptySet());
        assertTrue(parameterPath.isIn());
        assertTrue(parameterPath.isMissingInResult());
    }

    @Test
    public void testStartDate() {
        DictTree dict = tree.getMapNN("start-date");
        ParameterPath parameterPath = ParameterPath.parse(dict, molecule, this::property2term);
        assertNotNull(parameterPath);
        assertEquals(parameterPath.getPath(), singletonList(property2term("date")));
        assertEquals(parameterPath.getAtom().getName(), "Date");
        assertEquals(parameterPath.getAtomFilters().size(), 1);
        assertEquals(parameterPath.getAtomFilters().iterator().next().getSPARQLFilter(),
                     SPARQLFilter.build("FILTER($actual >= $input)"));
        assertFalse(parameterPath.isIn());
        assertFalse(parameterPath.isMissingInResult());
    }

    @Test
    public void testStartDateAssumeSPARQL() {
        DictTree dict = tree.getMapNN("start-date-assume-sparql");
        ParameterPath parameterPath = ParameterPath.parse(dict, molecule, this::property2term);
        assertNotNull(parameterPath);
        assertEquals(parameterPath.getAtomFilters().size(), 1);
        assertEquals(parameterPath.getAtomFilters().iterator().next().getSPARQLFilter(),
                     SPARQLFilter.build("FILTER($actual >= $input)"));
    }

    @DataProvider
    public static Object[][] invalidData() {
        return Stream.of("empty-path", "null-step", "non-existing")
                     .map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "invalidData")
    public void testInvalid(String name) {
        expectThrows(IllegalArgumentException.class, () ->
                ParameterPath.parse(tree.getMapNN(name), molecule, this::property2term));
    }

}