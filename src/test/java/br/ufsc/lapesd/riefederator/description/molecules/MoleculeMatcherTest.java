package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.testng.Assert.assertEquals;

public class MoleculeMatcherTest {
    public static @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static @Nonnull StdURI name = new StdURI(FOAF.name.getURI());
    public static @Nonnull StdURI age = new StdURI(FOAF.age.getURI());
    public static @Nonnull StdURI primaryTopic = new StdURI(FOAF.primaryTopic.getURI());
    public static @Nonnull StdURI Alice = new StdURI("http://example.org/Alice");
    public static @Nonnull StdVar X = new StdVar("x");
    public static @Nonnull StdVar Y = new StdVar("y");
    public static @Nonnull StdVar Z = new StdVar("z");
    public static @Nonnull StdVar W = new StdVar("w");

    /* ~~~ single-atom molecules ~~~ */

    public static @Nonnull Molecule person1o = Molecule.builder("Person1o")
            .out(name, new Atom("name")).build();
    public static @Nonnull Molecule person2o = Molecule.builder("Person2o")
            .out(name, new Atom("name")).out(age, new Atom("age")).build();
    public static @Nonnull Molecule person1i2o = Molecule.builder("Person1i2o")
            .in(primaryTopic, new Atom("Document"))
            .out(name, new Atom("name")).out(age, new Atom("age")).build();

    /* ~~~ same as above but disjoint ~~~ */

    public static @Nonnull Molecule person1od = Molecule.builder("Person1od")
            .out(name, new Atom("name"))
            .disjoint().build();
    public static @Nonnull Molecule person2od = Molecule.builder("Person2od")
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .disjoint().build();
    public static @Nonnull Molecule person2od1 = Molecule.builder("Person2od1")
            .out(name, Molecule.builder("name").nonDisjoint().buildAtom())
            .out(age, Molecule.builder("age").disjoint().buildAtom())
            .disjoint().build();
    public static @Nonnull Molecule person2od2 = Molecule.builder("Person2od2")
            .out(name, Molecule.builder("name").disjoint().buildAtom())
            .out(age, Molecule.builder("age").disjoint().buildAtom())
            .disjoint().build();

    /* ~~~ same as above but exclusive and closed ~~~ */

    public static @Nonnull Molecule person1oe = Molecule.builder("Person1oe")
            .out(name, new Atom("name"))
            .exclusive().closed().build();
    public static @Nonnull Molecule person2oe = Molecule.builder("Person2oe")
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .exclusive().closed().build();
    public static @Nonnull Molecule person1i2oe = Molecule.builder("Person1i2oe")
            .exclusive().closed()
            .in(primaryTopic, new Atom("Document"))
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .exclusive().closed().build();


    /* ~~~ add disjointness to the *e versions above ~~~ */

    public static @Nonnull Molecule person1oed = Molecule.builder("Person1oed")
            .out(name, new Atom("name"))
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed = Molecule.builder("Person2oed")
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed1 = Molecule.builder("Person2oed1")
            .out(name, Molecule.builder("name").nonDisjoint().buildAtom())
            .out(age, Molecule.builder("age").disjoint().buildAtom())
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person2oed2 = Molecule.builder("Person2oed2")
            .out(name, Molecule.builder("name").disjoint().buildAtom())
            .out(age, Molecule.builder("age").disjoint().buildAtom())
            .exclusive().closed().disjoint().build();
    public static @Nonnull Molecule person1i2oed = Molecule.builder("Person1i2oed")
            .exclusive().closed()
            .in(primaryTopic, new Atom("Document"))
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .exclusive().closed().disjoint().build();

    /* ~~~ large molecule ~~~ */
    private static StdURI worksFor = new StdURI("http://example.org/uni/worksFor");
    private static StdURI hostedBy = new StdURI("http://example.org/uni/hostedBy");
    private static StdURI enrolledAt = new StdURI("http://example.org/uni/enrolledAt");
    private static StdURI supervisor = new StdURI("http://example.org/uni/supervisor");
    private static StdURI supervised = new StdURI("http://example.org/uni/supervised");
    private static StdURI teaches = new StdURI("http://example.org/uni/teaches");
    private static StdURI hasProfessor = new StdURI("http://example.org/uni/hasProfessor");
    private static StdURI courseCode = new StdURI("http://example.org/uni/courseCode");
    private static StdURI takes = new StdURI("http://example.org/uni/takes");
    private static StdURI hasStudent = new StdURI("http://example.org/uni/hasStudent");
    private static StdURI advises = new StdURI("http://example.org/uni/advises");

    public static @Nonnull Molecule univ, unive;

    static {
        Atom name = Molecule.builder("name").disjoint().buildAtom();
        Atom age = Molecule.builder("age").disjoint().buildAtom();
        Atom supervision = Molecule.builder("Supervision").buildAtom();
        Atom professor = Molecule.builder("Professor")
                .out(MoleculeMatcherTest.name, name)
                .out(MoleculeMatcherTest.age, age)
                .in(supervisor, supervision)
                .in(supervised, supervision)
                .buildAtom();
        Atom course = Molecule.builder("Course")
                .in(teaches, professor)
                .out(hasProfessor, professor)
                .out(courseCode, Molecule.builder("courseCode").buildAtom())
                .buildAtom();
        Atom student = Molecule.builder("Student")
                .out(takes, course)
                .out(MoleculeMatcherTest.name, name)
                .out(MoleculeMatcherTest.age, age)
                .in(hasStudent, course)
                .in(advises, professor)
                .buildAtom();
        univ = Molecule.builder("University")
                .in(worksFor, professor)
                .in(hostedBy, course)
                .in(enrolledAt, student)
                .build();

        Atom supervisione = Molecule.builder("Supervision[e]")
                .exclusive().nonClosed()
                .buildAtom();
        Atom professore = Molecule.builder("Professor[e]")
                .out(MoleculeMatcherTest.name, name)
                .out(MoleculeMatcherTest.age, age)
                .in(supervisor, supervisione)
                .in(supervised, supervisione)
                .exclusive().closed()
                .buildAtom();
        Atom coursee = Molecule.builder("Course[e]")
                .in(teaches, professore)
                .out(hasProfessor, professore)
                .out(courseCode, Molecule.builder("courseCode").buildAtom())
                .exclusive().closed()
                .buildAtom();
        Atom studente = Molecule.builder("Student[e]")
                .out(takes, coursee)
                .out(MoleculeMatcherTest.name, name)
                .out(MoleculeMatcherTest.age, age)
                .in(hasStudent, coursee)
                .in(advises, professore)
                .exclusive().nonClosed()
                .buildAtom();
        unive = Molecule.builder("University[e]")
                .in(worksFor, professore)
                .in(hostedBy, coursee)
                .in(enrolledAt, studente)
                .exclusive().closed()
                .build();
    }

    /* ~~~ data methods for tests ~~~ */

    private static List<List<Object>> nonExclusiveMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1o, singletonList(new Triple(X, name, Y)),
                                 e, singleton(new Triple(X, name, Y))),
                asList(person1o, singletonList(new Triple(X, knows, Y)), e, e),
                asList(person1o, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                                 e, singletonList(new Triple(X, name, Y))),
                asList(person1o, singletonList(new Triple(X, Y, Z)),
                                 e, singletonList(new Triple(X, Y, Z))),
                //ok: neither "name" nor "person1o" is disjoint
                asList(person1o, singletonList(new Triple(X, name, X)),
                                   e, singleton(new Triple(X, name, X))),

                asList(person2o, singletonList(new Triple(X, name, Y)),
                                   e, singleton(new Triple(X, name, Y))),
                asList(person2o, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                                 e, newHashSet(new Triple(X, name, Y), new Triple(X, age, Z))),
                asList(person2o, asList(new Triple(X, knows, Y), new Triple(Y, age, Z)),
                                   e, singleton(new Triple(Y, age, Z))),
                asList(person2o, singletonList(new Triple(X, knows, Y)), e, e),
                asList(person2o, asList(new Triple(X, knows, Y), new Triple(Y, primaryTopic, X)),
                                 e, e),
                asList(person2o, singletonList(new Triple(X, Y, Z)),
                                 e, singletonList(new Triple(X, Y, Z))),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2o, singletonList(new Triple(X, name, Y)),
                                   e, singleton(new Triple(X, name, Y))),
                asList(person1i2o, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                                   e, newHashSet(new Triple(X, name, Y), new Triple(X, age, Z))),
                asList(person1i2o, asList(new Triple(X, knows, Y), new Triple(Y, age, Z)),
                                   e, singleton(new Triple(Y, age, Z))),
                asList(person1i2o, singletonList(new Triple(X, knows, Y)), e, e),
                asList(person1i2o, asList(new Triple(X, knows, Y), new Triple(Y, primaryTopic, X)),
                                   e, singleton(new Triple(Y, primaryTopic, X))),
                asList(person1i2o, singletonList(new Triple(X, Y, Z)),
                                   e, singletonList(new Triple(X, Y, Z))),

                /* test matching of the input link in person1i2o */
                asList(person1i2o, singletonList(new Triple(X, primaryTopic, Y)),
                                   e, singleton(new Triple(X, primaryTopic, Y))),
                asList(person1i2o, asList(new Triple(X, primaryTopic, Y), new Triple(X, name, Y)),
                                   e, newHashSet(new Triple(X, primaryTopic, Y),
                                                 new Triple(X, name, Y))),
                asList(person1i2o, asList(new Triple(X, primaryTopic, Y), new Triple(X, knows, Y)),
                                   e, singleton(new Triple(X, primaryTopic, Y)))
        );
    }

//    private static List<List<Object>> nonExclusiveDisjointMatchData() {
//        List<Object> e = emptyList();
//        return asList(
//                asList(person1od, singletonList(new Triple(X, name, Y)),
//                                  e, singleton(new Triple(X, name, Y))),
//                // do not fail because person is not exclusive & closed
//                asList(person1od, singletonList(new Triple(X, name, X)),
//                                  e, singleton(new Triple(X, name, X))),
//                // ok: no conflicts (and age is not described)
//                asList(person1od, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
//                                  e, singleton(new Triple(X, name, Y))),
//                // ok: age link is not described
//                asList(person1od, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                                  e, singleton(new Triple(X, name, Y))),
//                // ok: age is described, but no conflicts
//                asList(person2od, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
//                                  e, asList(new Triple(X, name, Y), new Triple(X, age, Z))),
//                // ok: "name" and "age" are not declared disjoint
//                asList(person2od, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                                  e, asList(new Triple(X, name, Y), new Triple(X, age, Y))),
//                // ok: "age" is declared disjoint but not as exclusive & closed
//                asList(person2od1, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                                   e, asList(new Triple(X, name, Y), new Triple(X, age, Y))),
//                // ok: "name" and "age" are disjoint but not exclusive&closed
//                asList(person2od2, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                                   e, asList(new Triple(X, name, Y), new Triple(X, age, Y)))
//        );
//    }

    private static List<List<Object>> exclusiveMatchData() {
        List<Object> e = emptyList();
        return asList(
                asList(person1oe, singletonList(new Triple(X, name, Y)),
                                  singleton(singleton(new Triple(X, name, Y))), e),

                // ok bcs neither Atom in the link is disjoint
                asList(person1oe, singletonList(new Triple(X, name, X)),
                                  singleton(singleton(new Triple(X, name, X))), e),
                // fails bcs foaf:knows is not in the molecule
                asList(person1oe, singletonList(new Triple(X, knows, Y)), e, e),
                // fails bcs AGE is not in molecule
                asList(person1oe, asList(new Triple(X, name, Y), new Triple(X, age, Z)), e, e),
                asList(person1oe, singletonList(new Triple(X, Y, Z)),
                                  singleton(singleton(new Triple(X, Y, Z))), e),

                asList(person2oe, singletonList(new Triple(X, name, Y)),
                                  singleton(singleton(new Triple(X, name, Y))), e),
                asList(person2oe, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                                  singleton(newHashSet(new Triple(X, name, Y),
                                                       new Triple(X, age, Z))), e),
                // ok bcs X and Z match independently
                asList(person2oe, asList(new Triple(X, name, Y), new Triple(Z, age, W)),
                                  asList(singleton(new Triple(X, name, Y)),
                                         singleton(new Triple(Z, age, W))), e),
                // fail bcs Y will not occur in other sources
                asList(person2oe, asList(new Triple(X, knows, Y), new Triple(Y, age, Z)), e, e),
                // fail for the same reason as non-exclusive
                asList(person2oe, singletonList(new Triple(X, knows, Y)), e, e),
                asList(person2oe, asList(new Triple(X, knows, Y), new Triple(Y, primaryTopic, X)),
                                  e, e),
                asList(person2oe, singletonList(new Triple(X, Y, Z)),
                                  singleton(singleton(new Triple(X, Y, Z))), e),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2oe, singletonList(new Triple(X, name, Y)),
                              singleton(singleton(new Triple(X, name, Y))), e),
                asList(person1i2oe, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                                    singleton(newHashSet(new Triple(X, name, Y),
                                                         new Triple(X, age, Z))), e),
                // fail bcs Y solutions would only occur in this source
                asList(person1i2oe, asList(new Triple(X, knows, Y), new Triple(Y, age, Z)), e, e),
                // fail bcs KNOWS does not occur in the molecule
                asList(person1i2oe, singletonList(new Triple(X, knows, Y)), e, e),
                // PRIMARY_TOPIC is in molecule, but KNOWS causes its elimination
                asList(person1i2oe, asList(new Triple(X, knows, Y),
                        new Triple(Y, primaryTopic, X)), e, e),
                asList(person1i2oe, singletonList(new Triple(X, Y, Z)),
                                    singleton(singleton(new Triple(X, Y, Z))), e),

                /* test matching of the input link in person1i2o */
                asList(person1i2oe, singletonList(new Triple(X, primaryTopic, Y)),
                        singleton(singleton(new Triple(X, primaryTopic, Y))), e),
                asList(person1i2oe, asList(new Triple(X, primaryTopic, Y),
                        new Triple(Y, age, Z),
                        new Triple(Y, name, W)),
                        singleton(newHashSet(new Triple(X, primaryTopic, Y),
                                new Triple(Y, age, Z),
                                new Triple(Y, name, W))), e),
//                // ok bcs X can be both Document and person, given that they are not disjoint
//                asList(person1i2oe, asList(new Triple(X, primaryTopic, Y),
//                                           new Triple(X, name, Y)),
//                                    singleton(asList(new Triple(X, primaryTopic, Y),
//                                                     new Triple(X, name, Y))), e),
                // fail bcs X KNOWS fail and Y matches a exclusive&complete atom
                asList(person1i2oe, asList(new Triple(X, primaryTopic, Y),
                        new Triple(X, knows, Y)), e, e)
        );
    }

//    private static List<List<Object>> exclusiveDisjointMatchData() {
//        List<Object> e = emptyList();
//        return asList(
//                asList(person1oed, singletonList(new Triple(X, name, Y)),
//                                   singleton(singleton(new Triple(X, name, Y))), e),
//                // fails because of disjointness
//                asList(person1oed, singletonList(new Triple(X, name, X)), e, e),
//                // fails because exclusive & closed
//                asList(person1oed, asList(new Triple(X, name, Y), new Triple(X, age, Z)), e, e),
//
//                // ok: age is described, but no conflicts
//                asList(person2oed, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
//                         singleton(asList(new Triple(X, name, Y), new Triple(X, age, Z))), e),
//                // ok: "name" and "age" are not declared disjoint
//                asList(person2oed, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                         singleton(asList(new Triple(X, name, Y), new Triple(X, age, Y))), e),
//                // fail: "age" is declared disjoint
//                asList(person2oed1, asList(new Triple(X, name, Y), new Triple(X, age, Y)), e, e),
//                // fail: "name" and "age" are declared disjoint
//                asList(person2oed2, asList(new Triple(X, name, Y), new Triple(X, age, Y)), e, e),
//
//                // ok: name and age are not declared disjoint
//                asList(person1i2oed, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                           singleton(asList(new Triple(X, name, Y), new Triple(X, age, Y))), e),
//                // fail: Document and Person1i2oed are disjoint
//                asList(person1i2oed, asList(new Triple(X, primaryTopic, Y),
//                                            new Triple(X, name, Y)),
//                                     e, e),
//                // ok: name and age are not declared disjoint
//                asList(person1i2oed, asList(new Triple(X, name, Y), new Triple(X, age, Y),
//                        new Triple(W, primaryTopic, X)),
//                        singleton(asList(new Triple(X, name, Y),
//                                new Triple(X, age, Y),
//                                new Triple(W, primaryTopic, X))), e),
//                // fail: X binds to both "person1i2oed" and "Document"
//                asList(person1i2oed, asList(new Triple(X, name, Y), new Triple(X, age, Y),
//                        new Triple(X, primaryTopic, X)),
//                        e, e)
//        );
//    }

    private static List<List<Object>>  nonExclusiveMatchDataOnLargeMolecules() {
        List<Object> e = emptyList();
        return asList(
                asList(univ, singletonList(new Triple(X, name, Y)),
                             e, singletonList(new Triple(X, name, Y))),
                asList(univ, singletonList(new Triple(X, Y, Z)),
                             e, singletonList(new Triple(X, Y, Z))),
                asList(univ, singletonList(new Triple(Alice, Y, Z)),
                             e, singletonList(new Triple(Alice, Y, Z))),
                asList(univ, singletonList(new Triple(X, advises, Y)),
                             e, singletonList(new Triple(X, advises, Y))),
                asList(univ, asList(new Triple(X, advises, Y), new Triple(Y, name, Z),
                                    new Triple(X, name, Z)),
                             e, asList(new Triple(X, advises, Y), new Triple(Y, name, Z),
                                       new Triple(X, name, Z)))
        );
    }


    private static List<List<Object>>  exclusiveMatchDataOnLargeMolecules() {
        List<Object> e = emptyList();
        return asList(
                asList(unive, singletonList(new Triple(X, name, Y)),
                              singleton(singleton(new Triple(X, name, Y))), e),
                asList(unive, singletonList(new Triple(X, Y, Z)),
                              singleton(singleton(new Triple(X, Y, Z))), e),
                asList(unive, singletonList(new Triple(Alice, Y, Z)),
                              singleton(singleton(new Triple(Alice, Y, Z))), e),
                asList(unive, singletonList(new Triple(X, advises, Y)),
                              singleton(singleton(new Triple(X, advises, Y))), e),
                asList(unive, asList(new Triple(X, name, Y), new Triple(X, age, Z)),
                              singleton(asList(new Triple(X, name, Y), new Triple(X, age, Z))), e),
//                // fails bcs name and age are disjoint atoms
//                asList(unive, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                              e, e),
                asList(unive, asList(new Triple(X, name, Y), new Triple(Z, advises, X),
                                     new Triple(Z, name, W)),
                              asList(asList(new Triple(X, name, Y),
                                            new Triple(Z, name, W),
                                            new Triple(Z, advises, X)),
                                     singletonList(new Triple(Z, name, W))), e)
        );
    }

    @Nonnull
    private static List<List<Object>> allMatchData() {
        List<List<Object>> list = new ArrayList<>();
        list.addAll(nonExclusiveMatchData());
//        list.addAll(nonExclusiveDisjointMatchData());
        list.addAll(exclusiveMatchData());
//        list.addAll(exclusiveDisjointMatchData());
        list.addAll(nonExclusiveMatchDataOnLargeMolecules());
        list.addAll(exclusiveMatchDataOnLargeMolecules());
        return list;
    }

    @DataProvider
    public static Object[][] matchData() {
        List<List<Object>> list = allMatchData();

        List<Function<Molecule, Description>> factories = asList(
                MoleculeMatcherWithDisjointness::new,
                m -> new MoleculeMatcher(m, new TransitiveClosureTBoxReasoner(new TBoxSpec()))
        );
        List<Object[]> arrays = new ArrayList<>();
        for (Function<Molecule, Description> factory : factories) {
            for (List<Object> row : list) {
                ArrayList<Object> copy = new ArrayList<>(row);
                copy.set(0, factory.apply((Molecule) copy.get(0)));
                arrays.add(copy.toArray());
            }
        }
        return arrays.toArray(new Object[0][]);
    }

    @DataProvider
    public static Object[][] semanticMatchData() {
        List<Object[]> arrays = new ArrayList<>();
        for (List<Object> row : allMatchData()) {
            TransitiveClosureTBoxReasoner reasoner =
                    new TransitiveClosureTBoxReasoner(new TBoxSpec());
            MoleculeMatcher matcher = new MoleculeMatcher((Molecule) row.get(0), reasoner);
            row.set(0, matcher);
            arrays.add(row.toArray());
        }
        return arrays.toArray(new Object[0][]);
    }

    /* ~~~ actual test methods ~~~ */

    @Test(dataProvider = "matchData")
    public void testMatch(@Nonnull Description description, @Nonnull List<Triple> queryAsList,
                          @Nonnull Collection<Collection<Triple>> exclusiveGroups,
                          @Nonnull Collection<Triple> nonExclusive) {
        CQuery query = CQuery.from(queryAsList);
        CQueryMatch match = description.match(query);
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
        try {
            assertEquals(actualGroups, exclusiveGroups.stream().map(Sets::newHashSet)
                                                      .collect(toSet()));
        } catch (AssertionError e) {
            if (description instanceof MoleculeMatcherWithDisjointness) {
                Set<Triple> set = exclusiveGroups.stream().flatMap(Collection::stream)
                                                          .collect(toSet());
                assertEquals(actualGroups, singleton(set));
            } else {
                throw e;
            }
        }
    }

    @Test(dataProvider = "semanticMatchData")
    public void testSemanticMatchEmptyTBox(@Nonnull SemanticDescription description,
                                           @Nonnull List<Triple> queryAsList,
                                           @Nonnull Collection<Collection<Triple>> exclusiveGroups,
                                           @Nonnull Collection<Triple> nonExclusive) {
        CQuery query = CQuery.from(queryAsList);
        SemanticCQueryMatch match = description.semanticMatch(query);
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

        // all alternative must match its EG
        for (CQuery eg : match.getKnownExclusiveGroups()) {
            for (CQuery alternative : match.getAlternatives(eg)) {
                assertEquals(alternative.getMatchedTriples(), eg.getSet());
            }
        }
    }
}
