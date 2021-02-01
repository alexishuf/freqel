package br.ufsc.lapesd.freqel.description.molecules;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class MoleculeBuilderTest implements TestContext {
    public static final @Nonnull Atom a1 = new Atom("a");
    public static final @Nonnull Atom b  = new Atom("b");
    public static final @Nonnull Atom c  = new Atom("c");

    @Test
    public void testBuildAtomOnlyIn() {
        Atom core = new MoleculeBuilder("core").exclusive(true).in(knows, a1).buildAtom();
        assertEquals(core.getName(), "core");
        assertTrue(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false, emptySet())));
        assertEquals(core.getOut(), emptySet());
    }

    @Test
    public void testBuildAtomInOut() {
        Atom core = new MoleculeBuilder("core").exclusive(false).in(knows, a1)
                .out(age, b).outAuthoritative(age, c).buildAtom();
        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false, emptySet())));
        assertEquals(core.getOut(), newHashSet(new MoleculeLink(age, b, false, emptySet()),
                                               new MoleculeLink(age, c, true, emptySet())));
    }

    @Test
    public void testBuildAtomThenMolecule() {
        MoleculeBuilder builder = new MoleculeBuilder("core");
        Atom core = builder.exclusive(false).in(knows, a1).out(age, b).buildAtom();
        Molecule molecule = builder.build();

        assertEquals(core.getName(), "core");
        assertFalse(core.isExclusive());
        assertEquals(core.getIn(), singleton(new MoleculeLink(knows, a1, false, emptySet())));
        assertEquals(core.getOut(), singleton(new MoleculeLink(age, b, false, emptySet())));

        assertEquals(molecule.getCore(), core);
    }

    @Test
    public void testBuildMultiCoreMolecule() {
        Supplier<Molecule> supplier =
                () -> Molecule.builder("core1").exclusive().closed().out(knows, b)
                        .startNewCore("core2").out(age, a1)
                        .filter(AtomFilter.builder(SPARQLFilterFactory.parseFilter("$actual > $input"))
                                .map(AtomRole.OUTPUT.wrap(a1), "actual")
                                .map(AtomRole.INPUT.wrap(a1), "input")
                                .build())
                        .build();
        Molecule m = supplier.get();
        assertEquals(m.getCore().getName(), "core1");
        assertEquals(m.getCores().stream().map(Atom::getName).collect(toList()),
                     asList("core1", "core2"));
        assertEquals(m.getCores(),
                     asList(m.getAtom("core1"), m.getAtom("core2")));
        assertNotEquals(m.getAtom("core1"), m.getAtom("core2"));


        assertEquals(m.getFiltersWithAtom("core1"), emptySet());
        assertEquals(m.getFiltersWithAtom("core2"), emptySet());
        assertEquals(m.getFiltersWithAtom(a1.getName()).size(), 1);
        AtomFilter filter = m.getFiltersWithAtom(a1.getName()).iterator().next();
        assertEquals(filter.getSPARQLFilter(), SPARQLFilterFactory.parseFilter("$actual > $input"));

        assertEquals(m, supplier.get());
        assertEquals(m.hashCode(), supplier.get().hashCode());
    }
}