package br.ufsc.lapesd.freqel.linkedator.strategies.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.query.SimplePath;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class AtomPathTest implements TestContext {

    private static final Molecule Person = Molecule.builder("Person")
            .out(age, new Atom("age"))
            .out(name, new Atom("name"))
            .in(knows, new Atom("Friend"))
            .in(author, Molecule.builder("Book")
                                .in(isPrimaryTopicOf, new Atom("Thing"))
                                .out(authorName, new Atom("AuthorName")).buildAtom())
            .build();

    @DataProvider
    public static @Nonnull Object[][] simplePathData() {
        return Stream.of(
                asList(Person, AtomPath.EMPTY, SimplePath.EMPTY),
                asList(Person, singletonList("Person"), SimplePath.EMPTY),
                asList(Person, asList("Person", "age"), SimplePath.to(age).build()),
                asList(Person, asList("Person", "Book"), SimplePath.from(author).build()),
                asList(Person, asList("Person", "Book", "AuthorName"),
                       SimplePath.from(author).to(authorName).build()),
                asList(Person, asList("Person", "Book", "Thing"),
                       SimplePath.from(author).from(isPrimaryTopicOf).build())
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "simplePathData")
    public void testToSimplePath(Molecule molecule, List<String> path, SimplePath expected) {
        AtomPath atomPath = new AtomPath(path);
        assertEquals(atomPath.toSimplePath(molecule), expected);
    }

    @Test
    public void testToSimplePathBadAtoms() {
        expectThrows(IllegalArgumentException.class,
                     () -> new AtomPath(asList("Book", "Orange")).toSimplePath(Person));
    }

    @Test
    public void testStartingAt() {
        AtomPath path = new AtomPath(asList("A", "B", "C"));
        assertEquals(path.startingAt("C"), new AtomPath(singletonList("C")));
        assertEquals(path.startingAt("B"), new AtomPath(asList("B", "C")));
        assertEquals(path.startingAt("A"), new AtomPath(asList("A", "B", "C")));
        assertNull(path.startingAt("D"));
        assertNull(AtomPath.EMPTY.startingAt("A"));
    }

    @Test
    public void testUnmodifiable() {
        AtomPath path = new AtomPath(asList("A", "B", "C", "D"));
        expectThrows(UnsupportedOperationException.class, () -> path.add("E"));
        expectThrows(UnsupportedOperationException.class, () -> path.addAll(asList("E", "F")));
        expectThrows(UnsupportedOperationException.class, () -> path.removeIf(Objects::nonNull));
        expectThrows(UnsupportedOperationException.class, () -> path.removeAll(singleton("A")));
        expectThrows(UnsupportedOperationException.class, path::clear);
    }
}