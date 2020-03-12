package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class MoleculeTest implements TestContext {
    public static final @Nonnull Atom a1 = new Atom("a");
    public static final @Nonnull Atom a2 = new Atom("a");
    public static final @Nonnull Atom b  = new Atom("b");
    public static final @Nonnull Atom c  = new Atom("c");

    @DataProvider
    public static Object[][] equalsData() {
        return new Object[][] {
                new Object[] {new Molecule(a1), new Molecule(a2), true},
                new Object[] {new Molecule(a1), new Molecule(b), false},
        };
    }

    @Test(dataProvider = "equalsData")
    public void testEquals(Molecule left, Molecule right, boolean expected) {
        if (expected) {
            assertEquals(left, right);
            assertEquals(left.hashCode(), right.hashCode());
        } else {
            assertNotEquals(left, right);
        }
    }

    @Test
    public void testIndex() {
        Atom professor = Molecule.builder("Professor")
                .out(name, new Atom("ProfessorName"))
                .out(age, new Atom("ProfessorAge"))
                .out(bornIn, new Atom("ProfessorCity"))
                .buildAtom();
        Molecule molecule = Molecule.builder("Student")
                .out(name, new Atom("StudentName"))
                .in(knows, professor)
                .out(knows, professor)
                .out(likes, professor)
                .build();

        Molecule.Index index = molecule.getIndex();
        Set<List<Object>> expected = new HashSet<>();
        expected.add(asList("Student", knows, "Professor"));
        expected.add(asList("Professor", knows, "Student"));
        assertEquals(index.stream(null, knows, null).map(Molecule.Triple::asList)
                          .collect(toSet()),
                     expected);

        assertEquals(index.stream("Student", knows, null).map(Molecule.Triple::asList)
                          .collect(toList()),
                     singletonList(asList("Student", knows, "Professor")));
        assertEquals(index.stream(null, knows, "Professor").map(Molecule.Triple::asList)
                          .collect(toList()),
                     singletonList(asList("Student", knows, "Professor")));
        assertEquals(index.stream("Student", knows, "Professor").map(Molecule.Triple::asList)
                          .collect(toList()),
                     singletonList(asList("Student", knows, "Professor")));
        assertEquals(index.stream("Student", knows, "Student").map(Molecule.Triple::asList)
                        .collect(toList()),
                     emptyList());

        assertEquals(index.get(null, likes, "Professor").stream().map(Molecule.Triple::asList)
                                                                  .collect(toList()),
                     singletonList(asList("Student", likes, "Professor")));
        assertEquals(index.get("Professor", likes, null).stream().map(Molecule.Triple::asList)
                                                                  .collect(toList()),
                     emptyList());
    }

}