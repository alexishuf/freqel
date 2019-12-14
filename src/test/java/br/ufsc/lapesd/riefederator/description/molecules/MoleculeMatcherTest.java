package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class MoleculeMatcherTest {
    public static @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static @Nonnull StdURI NAME = new StdURI(FOAF.name.getURI());
    public static @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static @Nonnull StdURI PRIMARY_TOPIC = new StdURI(FOAF.primaryTopic.getURI());
    public static @Nonnull StdVar X = new StdVar("x");
    public static @Nonnull StdVar Y = new StdVar("y");
    public static @Nonnull StdVar Z = new StdVar("z");
    public static @Nonnull StdVar W = new StdVar("w");

    /* ~~~ single-atom molecules ~~~ */

    public static @Nonnull Molecule person1o = Molecule.builder("Person1o")
            .out(NAME, new Atom("name")).build();
    public static @Nonnull Molecule person2o = Molecule.builder("Person2o")
            .out(NAME, new Atom("name")).out(AGE, new Atom("age")).build();
    public static @Nonnull Molecule person1i2o = Molecule.builder("Person1i2o")
            .in(PRIMARY_TOPIC, new Atom("Document"))
            .out(NAME, new Atom("name")).out(AGE, new Atom("age")).build();

    /* ~~~ same as above but disjoint ~~~ */

    public static @Nonnull Molecule person1od = Molecule.builder("Person1od")
            .out(NAME, new Atom("name"))
            .disjoint().build();
    public static @Nonnull Molecule person2od = Molecule.builder("Person2od")
            .out(NAME, new Atom("name")).out(AGE, new Atom("age"))
            .disjoint().build();
    public static @Nonnull Molecule person2od1 = Molecule.builder("Person2od1")
            .out(NAME, Molecule.builder("name").nonDisjoint().buildAtom())
            .out(AGE, Molecule.builder("age").disjoint().buildAtom())
            .disjoint().build();
    public static @Nonnull Molecule person2od2 = Molecule.builder("Person2od2")
            .out(NAME, Molecule.builder("name").disjoint().buildAtom())
            .out(AGE, Molecule.builder("age").disjoint().buildAtom())
            .disjoint().build();

    /* ~~~ same as above but exclusive and closed ~~~ */

    public static @Nonnull Molecule person1oe = Molecule.builder("Person1oe")
            .out(NAME, new Atom("name"))
            .exclusive().closed().build();
    public static @Nonnull Molecule person2oe = Molecule.builder("Person2oe")
            .out(NAME, new Atom("name")).out(AGE, new Atom("age"))
            .exclusive().closed().build();
    public static @Nonnull Molecule person1i2oe = Molecule.builder("Person1i2oe")
            .exclusive().closed()
            .in(PRIMARY_TOPIC, new Atom("Document"))
            .out(NAME, new Atom("name")).out(AGE, new Atom("age"))
            .exclusive().closed().build();


    /* ~~~ add disjointness to the *e versions above ~~~ */

    public static @Nonnull Molecule person1oed = Molecule.builder("Person1oed")
            .out(NAME, new Atom("name"))
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed = Molecule.builder("Person2oed")
            .out(NAME, new Atom("name")).out(AGE, new Atom("age"))
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed1 = Molecule.builder("Person2oed1")
            .out(NAME, Molecule.builder("name").nonDisjoint().buildAtom())
            .out(AGE, Molecule.builder("age").disjoint().buildAtom())
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed2 = Molecule.builder("Person2oed2")
            .out(NAME, Molecule.builder("name").disjoint().buildAtom())
            .out(AGE, Molecule.builder("age").disjoint().buildAtom())
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person1i2oed = Molecule.builder("Person1i2oed")
            .exclusive().closed()
            .in(PRIMARY_TOPIC, new Atom("Document"))
            .out(NAME, new Atom("name")).out(AGE, new Atom("age"))
            .exclusive().closed().disjoint().build();

    /* ~~~ data methods for tests ~~~ */

    private static List<List<Object>> nonExclusiveMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1o, singletonList(new Triple(X, NAME, Y)),
                        e, singleton(new Triple(X, NAME, Y))),
                asList(person1o, singletonList(new Triple(X, KNOWS, Y)), e, e),
                asList(person1o, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                        e, singletonList(new Triple(X, NAME, Y))),
                //ok: neither "name" nor "person1o" is disjoint
                asList(person1o, singletonList(new Triple(X, NAME, X)),
                        e, singleton(new Triple(X, NAME, X))),

                asList(person2o, singletonList(new Triple(X, NAME, Y)),
                        e, singleton(new Triple(X, NAME, Y))),
                asList(person2o, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                        e, newHashSet(new Triple(X, NAME, Y), new Triple(X, AGE, Z))),
                asList(person2o, asList(new Triple(X, KNOWS, Y), new Triple(Y, AGE, Z)),
                        e, singleton(new Triple(Y, AGE, Z))),
                asList(person2o, singletonList(new Triple(X, KNOWS, Y)), e, e),
                asList(person2o, asList(new Triple(X, KNOWS, Y), new Triple(Y, PRIMARY_TOPIC, X)),
                        e, e),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2o, singletonList(new Triple(X, NAME, Y)),
                        e, singleton(new Triple(X, NAME, Y))),
                asList(person1i2o, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                        e, newHashSet(new Triple(X, NAME, Y), new Triple(X, AGE, Z))),
                asList(person1i2o, asList(new Triple(X, KNOWS, Y), new Triple(Y, AGE, Z)),
                        e, singleton(new Triple(Y, AGE, Z))),
                asList(person1i2o, singletonList(new Triple(X, KNOWS, Y)), e, e),
                asList(person1i2o, asList(new Triple(X, KNOWS, Y), new Triple(Y, PRIMARY_TOPIC, X)),
                        e, singleton(new Triple(Y, PRIMARY_TOPIC, X))),

                /* test matching of the input link in person1i2o */
                asList(person1i2o, singletonList(new Triple(X, PRIMARY_TOPIC, Y)),
                        e, singleton(new Triple(X, PRIMARY_TOPIC, Y))),
                asList(person1i2o, asList(new Triple(X, PRIMARY_TOPIC, Y), new Triple(X, NAME, Y)),
                        e, newHashSet(new Triple(X, PRIMARY_TOPIC, Y), new Triple(X, NAME, Y))),
                asList(person1i2o, asList(new Triple(X, PRIMARY_TOPIC, Y), new Triple(X, KNOWS, Y)),
                        e, singleton(new Triple(X, PRIMARY_TOPIC, Y)))
        );
    }

    private static List<List<Object>> nonExclusiveDisjointMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1od, singletonList(new Triple(X, NAME, Y)),
                                  e, singleton(new Triple(X, NAME, Y))),
                // fails because of disjointness
                asList(person1od, singletonList(new Triple(X, NAME, X)), e, e),
                // ok: no conflicts (and age is not described)
                asList(person1od, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                                  e, singleton(new Triple(X, NAME, Y))),
                // ok: age link is not described
                asList(person1od, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)),
                                  e, singleton(new Triple(X, NAME, Y))),
                // ok: age is described, but no conflicts
                asList(person2od, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                                  e, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z))),
                // ok: "name" and "age" are not declared disjoint
                asList(person2od, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)),
                                  e, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y))),
                // fail: "age" is declared disjoint
                asList(person2od1, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)), e, e),
                // fail: "name" and "age" are declared disjoint
                asList(person2od2, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)), e, e)
        );
    }

    private static List<List<Object>> exclusiveMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1oe, singletonList(new Triple(X, NAME, Y)),
                                  singleton(singleton(new Triple(X, NAME, Y))), e),

                // ok bcs neither Atom in the link is disjoint
                asList(person1oe, singletonList(new Triple(X, NAME, X)),
                                  singleton(singleton(new Triple(X, NAME, X))), e),
                // fails bcs foaf:knows is not in the molecule
                asList(person1oe, singletonList(new Triple(X, KNOWS, Y)), e, e),
                // fails bcs AGE is not in molecule
                asList(person1oe, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)), e, e),

                asList(person2oe, singletonList(new Triple(X, NAME, Y)),
                                  singleton(singleton(new Triple(X, NAME, Y))), e),
                asList(person2oe, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                                  singleton(newHashSet(new Triple(X, NAME, Y),
                                                       new Triple(X, AGE, Z))), e),
                // ok bcs X and Z match independently
                asList(person2oe, asList(new Triple(X, NAME, Y), new Triple(Z, AGE, W)),
                                  asList(singleton(new Triple(X, NAME, Y)),
                                         singleton(new Triple(Z, AGE, W))), e),
                // fail bcs Y will not occur in other sources
                asList(person2oe, asList(new Triple(X, KNOWS, Y), new Triple(Y, AGE, Z)), e, e),
                // fail for the same reason as non-exclusive
                asList(person2oe, singletonList(new Triple(X, KNOWS, Y)), e, e),
                asList(person2oe, asList(new Triple(X, KNOWS, Y), new Triple(Y, PRIMARY_TOPIC, X)),
                        e, e),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2oe, singletonList(new Triple(X, NAME, Y)),
                              singleton(singleton(new Triple(X, NAME, Y))), e),
                asList(person1i2oe, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                                    singleton(newHashSet(new Triple(X, NAME, Y),
                                                         new Triple(X, AGE, Z))), e),
                // fail bcs Y solutions would only occur in this source
                asList(person1i2oe, asList(new Triple(X, KNOWS, Y), new Triple(Y, AGE, Z)), e, e),
                // fail bcs KNOWS does not occur in the molecule
                asList(person1i2oe, singletonList(new Triple(X, KNOWS, Y)), e, e),
                // PRIMARY_TOPIC is in molecule, but KNOWS causes its elimination
                asList(person1i2oe, asList(new Triple(X, KNOWS, Y),
                        new Triple(Y, PRIMARY_TOPIC, X)), e, e),

                /* test matching of the input link in person1i2o */
                asList(person1i2oe, singletonList(new Triple(X, PRIMARY_TOPIC, Y)),
                        singleton(singleton(new Triple(X, PRIMARY_TOPIC, Y))), e),
                asList(person1i2oe, asList(new Triple(X, PRIMARY_TOPIC, Y),
                        new Triple(Y, AGE, Z),
                        new Triple(Y, NAME, W)),
                        singleton(newHashSet(new Triple(X, PRIMARY_TOPIC, Y),
                                new Triple(Y, AGE, Z),
                                new Triple(Y, NAME, W))), e),
                // ok bcs X can be both Document and person, given that they are not disjoint
                asList(person1i2oe, asList(new Triple(X, PRIMARY_TOPIC, Y),
                                           new Triple(X, NAME, Y)),
                                    singleton(asList(new Triple(X, PRIMARY_TOPIC, Y),
                                                     new Triple(X, NAME, Y))), e),
                // fail bcs X KNOWS fail and Y matches a exclusive&complete atom
                asList(person1i2oe, asList(new Triple(X, PRIMARY_TOPIC, Y),
                        new Triple(X, KNOWS, Y)), e, e)
        );
    }

    private static List<List<Object>> exclusiveDisjointMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1oed, singletonList(new Triple(X, NAME, Y)),
                                   singleton(singleton(new Triple(X, NAME, Y))), e),
                // fails because of disjointness
                asList(person1oed, singletonList(new Triple(X, NAME, X)), e, e),
                // fails because exclusive & closed
                asList(person1oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)), e, e),

                // ok: age is described, but no conflicts
                asList(person2oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z)),
                         singleton(asList(new Triple(X, NAME, Y), new Triple(X, AGE, Z))), e),
                // ok: "name" and "age" are not declared disjoint
                asList(person2oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)),
                         singleton(asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y))), e),
                // fail: "age" is declared disjoint
                asList(person2oed1, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)), e, e),
                // fail: "name" and "age" are declared disjoint
                asList(person2oed2, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)), e, e),

                // ok: name and age are not declared disjoint
                asList(person1i2oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y)),
                           singleton(asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y))), e),
                // fail: Document and Person1i2oed are disjoint
                asList(person1i2oed, asList(new Triple(X, PRIMARY_TOPIC, Y),
                                            new Triple(X, NAME, Y)),
                                     e, e),
                // ok: name and age are not declared disjoint
                asList(person1i2oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y),
                        new Triple(W, PRIMARY_TOPIC, X)),
                        singleton(asList(new Triple(X, NAME, Y),
                                new Triple(X, AGE, Y),
                                new Triple(W, PRIMARY_TOPIC, X))), e),
                // fail: X binds to both "person1i2oed" and "Document"
                asList(person1i2oed, asList(new Triple(X, NAME, Y), new Triple(X, AGE, Y),
                        new Triple(X, PRIMARY_TOPIC, X)),
                        e, e)
        );
    }

    @DataProvider
    public static Object[][] matchData() {
        List<List<Object>> list = new ArrayList<>();
        list.addAll(nonExclusiveMatchData());
        list.addAll(nonExclusiveDisjointMatchData());
        list.addAll(exclusiveMatchData());
        list.addAll(exclusiveDisjointMatchData());
        // TODO more tests
//        list.addAll(nonExclusiveMatchDataOnLargeMolecules());
//        list.addAll(exclusiveMatchDataOnLargeMolecules());
        return list.stream().map(List::toArray).toArray(Object[][]::new);
    }

    /* ~~~ actual test methods ~~~ */

    @Test(dataProvider = "matchData")
    public void testMatch(@Nonnull Molecule molecule, @Nonnull List<Triple> queryAsList,
                          @Nonnull Collection<Collection<Triple>> exclusiveGroups,
                          @Nonnull Collection<Triple> nonExclusive) {
        CQuery query = CQuery.from(queryAsList);
        MoleculeMatcher matcher = new MoleculeMatcher(molecule);
        CQueryMatch match = matcher.match(query);
        assertEquals(match.getQuery(), query);

        // compare relevant & irrelevant triple patterns
        assertEquals(newHashSet(match.getAllRelevant()), concat(nonExclusive.stream(),
                exclusiveGroups.stream().flatMap(Collection::stream)).collect(toSet()));
        HashSet<Triple> irrelevant = newHashSet(match.getAllRelevant());
        irrelevant.retainAll(match.getIrrelevant());
        assertEquals(irrelevant, emptySet());

        // compare exclusive groups. Ignore ordering
        Set<Set<Triple>> actualGroups = match.getKnownExclusiveGroups().stream()
                .map(Sets::newHashSet).collect(toSet());
        assertEquals(actualGroups, exclusiveGroups.stream().map(Sets::newHashSet).collect(toSet()));
    }

    @Test
    public void testTODO() {
        fail("Add extra cases to matchData with larger molecules");
    }
}
