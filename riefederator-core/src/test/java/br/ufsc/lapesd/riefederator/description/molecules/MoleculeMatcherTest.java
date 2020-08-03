package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class MoleculeMatcherTest implements TestContext {

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

    public static @Nonnull Molecule person2oe1f = Molecule.builder("Person2oe1f")
            .out(name, new Atom("name"))
            .out(age, new Atom("age"))
            .filter(AtomFilter.builder("?ac >= ?in")
                    .map(AtomRole.INPUT.wrap("age"), "in")
                    .map(AtomRole.OUTPUT.wrap("age"), "ac").buildFilter())
            .exclusive().closed().build();

    public static @Nonnull Molecule person1i2oe1f = Molecule.builder("Person2oe1f")
            .in(primaryTopic,
                    Molecule.builder("Document")
                            .out(title, new Atom("title"))
                            .exclusive().closed().buildAtom())
            .out(name, new Atom("name"))
            .out(age, new Atom("age"))
            .filter(AtomFilter.builder("?ac >= ?in")
                    .map(AtomRole.INPUT.wrap("age"), "in")
                    .map(AtomRole.OUTPUT.wrap("age"), "ac").buildFilter())
            .exclusive().closed().build();

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
                asList(person1o, singletonList(new Triple(x, name, y)),
                                 e, singleton(new Triple(x, name, y))),
                asList(person1o, singletonList(new Triple(x, knows, y)), e, e),
                asList(person1o, asList(new Triple(x, name, y), new Triple(x, age, z)),
                                 e, singletonList(new Triple(x, name, y))),
                asList(person1o, singletonList(new Triple(x, y, z)),
                                 e, singletonList(new Triple(x, y, z))),
                //ok: neither "name" nor "person1o" is disjoint
                asList(person1o, singletonList(new Triple(x, name, x)),
                                   e, singleton(new Triple(x, name, x))),

                asList(person2o, singletonList(new Triple(x, name, y)),
                                   e, singleton(new Triple(x, name, y))),
                asList(person2o, asList(new Triple(x, name, y), new Triple(x, age, z)),
                                 e, newHashSet(new Triple(x, name, y), new Triple(x, age, z))),
                asList(person2o, asList(new Triple(x, knows, y), new Triple(y, age, z)),
                                   e, singleton(new Triple(y, age, z))),
                asList(person2o, singletonList(new Triple(x, knows, y)), e, e),
                asList(person2o, asList(new Triple(x, knows, y), new Triple(y, primaryTopic, x)),
                                 e, e),
                asList(person2o, singletonList(new Triple(x, y, z)),
                                 e, singletonList(new Triple(x, y, z))),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2o, singletonList(new Triple(x, name, y)),
                                   e, singleton(new Triple(x, name, y))),
                asList(person1i2o, asList(new Triple(x, name, y), new Triple(x, age, z)),
                                   e, newHashSet(new Triple(x, name, y), new Triple(x, age, z))),
                asList(person1i2o, asList(new Triple(x, knows, y), new Triple(y, age, z)),
                                   e, singleton(new Triple(y, age, z))),
                asList(person1i2o, singletonList(new Triple(x, knows, y)), e, e),
                asList(person1i2o, asList(new Triple(x, knows, y), new Triple(y, primaryTopic, x)),
                                   e, singleton(new Triple(y, primaryTopic, x))),
                asList(person1i2o, singletonList(new Triple(x, y, z)),
                                   e, singletonList(new Triple(x, y, z))),

                /* test matching of the input link in person1i2o */
                asList(person1i2o, singletonList(new Triple(x, primaryTopic, y)),
                                   e, singleton(new Triple(x, primaryTopic, y))),
                asList(person1i2o, asList(new Triple(x, primaryTopic, y), new Triple(x, name, y)),
                                   e, newHashSet(new Triple(x, primaryTopic, y),
                                                 new Triple(x, name, y))),
                asList(person1i2o, asList(new Triple(x, primaryTopic, y), new Triple(x, knows, y)),
                                   e, singleton(new Triple(x, primaryTopic, y)))
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
                asList(person1oe, singletonList(new Triple(x, name, y)),
                                  singleton(singleton(new Triple(x, name, y))), e),

                // ok bcs neither Atom in the link is disjoint
                asList(person1oe, singletonList(new Triple(x, name, x)),
                                  singleton(singleton(new Triple(x, name, x))), e),
                // fails bcs foaf:knows is not in the molecule
                asList(person1oe, singletonList(new Triple(x, knows, y)), e, e),
                // fails bcs AGE is not in molecule
                asList(person1oe, asList(new Triple(x, name, y), new Triple(x, age, z)), e, e),
                asList(person1oe, singletonList(new Triple(x, y, z)),
                                  singleton(singleton(new Triple(x, y, z))), e),

                asList(person2oe, singletonList(new Triple(x, name, y)),
                                  singleton(singleton(new Triple(x, name, y))), e),
                asList(person2oe, asList(new Triple(x, name, y), new Triple(x, age, z)),
                                  singleton(newHashSet(new Triple(x, name, y),
                                                       new Triple(x, age, z))), e),
                // ok bcs X and Z match independently
                asList(person2oe, asList(new Triple(x, name, y), new Triple(z, age, w)),
                                  asList(singleton(new Triple(x, name, y)),
                                         singleton(new Triple(z, age, w))), e),
                // fail bcs Y will not occur in other sources
                asList(person2oe, asList(new Triple(x, knows, y), new Triple(y, age, z)), e, e),
                // fail for the same reason as non-exclusive
                asList(person2oe, singletonList(new Triple(x, knows, y)), e, e),
                asList(person2oe, asList(new Triple(x, knows, y), new Triple(y, primaryTopic, x)),
                                  e, e),
                asList(person2oe, singletonList(new Triple(x, y, z)),
                                  singleton(singleton(new Triple(x, y, z))), e),

                /* the extra input in person1i2o should not change the person2o results */
                asList(person1i2oe, singletonList(new Triple(x, name, y)),
                              singleton(singleton(new Triple(x, name, y))), e),
                asList(person1i2oe, asList(new Triple(x, name, y), new Triple(x, age, z)),
                                    singleton(newHashSet(new Triple(x, name, y),
                                                         new Triple(x, age, z))), e),
                // fail bcs Y solutions would only occur in this source
                asList(person1i2oe, asList(new Triple(x, knows, y), new Triple(y, age, z)), e, e),
                // fail bcs KNOWS does not occur in the molecule
                asList(person1i2oe, singletonList(new Triple(x, knows, y)), e, e),
                // PRIMARY_TOPIC is in molecule, but KNOWS causes its elimination
                asList(person1i2oe, asList(new Triple(x, knows, y),
                        new Triple(y, primaryTopic, x)), e, e),
                asList(person1i2oe, singletonList(new Triple(x, y, z)),
                                    singleton(singleton(new Triple(x, y, z))), e),

                /* test matching of the input link in person1i2o */
                asList(person1i2oe, singletonList(new Triple(x, primaryTopic, y)),
                        singleton(singleton(new Triple(x, primaryTopic, y))), e),
                asList(person1i2oe, asList(new Triple(x, primaryTopic, y),
                        new Triple(y, age, z),
                        new Triple(y, name, w)),
                        singleton(newHashSet(new Triple(x, primaryTopic, y),
                                new Triple(y, age, z),
                                new Triple(y, name, w))), e),
//                // ok bcs X can be both Document and person, given that they are not disjoint
//                asList(person1i2oe, asList(new Triple(X, primaryTopic, Y),
//                                           new Triple(X, name, Y)),
//                                    singleton(asList(new Triple(X, primaryTopic, Y),
//                                                     new Triple(X, name, Y))), e),
                // fail bcs X KNOWS fail and Y matches a exclusive&complete atom
                asList(person1i2oe, asList(new Triple(x, primaryTopic, y),
                        new Triple(x, knows, y)), e, e)
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
                asList(univ, singletonList(new Triple(x, name, y)),
                             e, singletonList(new Triple(x, name, y))),
                asList(univ, singletonList(new Triple(x, y, z)),
                             e, singletonList(new Triple(x, y, z))),
                asList(univ, singletonList(new Triple(Alice, y, z)),
                             e, singletonList(new Triple(Alice, y, z))),
                asList(univ, singletonList(new Triple(x, advises, y)),
                             e, singletonList(new Triple(x, advises, y))),
                asList(univ, asList(new Triple(x, advises, y), new Triple(y, name, z),
                                    new Triple(x, name, z)),
                             e, asList(new Triple(x, advises, y), new Triple(y, name, z),
                                       new Triple(x, name, z)))
        );
    }


    private static List<List<Object>>  exclusiveMatchDataOnLargeMolecules() {
        List<Object> e = emptyList();
        return asList(
                asList(unive, singletonList(new Triple(x, name, y)),
                              singleton(singleton(new Triple(x, name, y))), e),
                asList(unive, singletonList(new Triple(x, y, z)),
                              singleton(singleton(new Triple(x, y, z))), e),
                asList(unive, singletonList(new Triple(Alice, y, z)),
                              singleton(singleton(new Triple(Alice, y, z))), e),
                asList(unive, singletonList(new Triple(x, advises, y)),
                              singleton(singleton(new Triple(x, advises, y))), e),
                asList(unive, asList(new Triple(x, name, y), new Triple(x, age, z)),
                              singleton(asList(new Triple(x, name, y), new Triple(x, age, z))), e),
//                // fails bcs name and age are disjoint atoms
//                asList(unive, asList(new Triple(X, name, Y), new Triple(X, age, Y)),
//                              e, e),
                asList(unive, asList(new Triple(x, name, y), new Triple(z, advises, x),
                                     new Triple(z, name, w)),
                              asList(asList(new Triple(x, name, y),
                                            new Triple(z, name, w),
                                            new Triple(z, advises, x)),
                                     singletonList(new Triple(z, name, w))), e)
        );
    }

    private static List<List<Object>> exclusiveMatchDataWithFilters() {
        return asList(
                // exact match with the description
                asList(person2oe1f,
                       createQuery(x, name, y,
                                   x, age, z, SPARQLFilter.build("?z >= 23")),
                       singleton(createQuery(x, name, y,
                                             x, age, z,
                                             SPARQLFilter.build("?z >= 23"))),
                       emptyList()),
                // match only age, leave name out ...
                asList(person2oe1f,
                       createQuery(x, age, z, SPARQLFilter.build("?z >= 23")),
                       singleton(createQuery(x, age, z, SPARQLFilter.build("?z >= 23"))),
                       emptyList()),
                // match all output links with molecule that has input...
                asList(person1i2oe1f,
                       createQuery(x, name, y,
                                   x, age, z, SPARQLFilter.build("?z >= 23")),
                       singleton(createQuery(x, name, y,
                                             x, age, z, SPARQLFilter.build("?z >= 23"))),
                       emptyList()),
                // complete match with  molecule that has input...
                asList(person1i2oe1f,
                        createQuery(x, name, y,
                                    w, primaryTopic, x,
                                    w, title, u,
                                    x, age, z, SPARQLFilter.build("?z >= 23")),
                        singleton(createQuery(x, name, y,
                                              w, primaryTopic, x,
                                              w, title, u,
                                              x, age, z, SPARQLFilter.build("?z >= 23"))),
                        emptyList()),
                // match only age on simple molecule but provide subsuming filter
                asList(person2oe1f,
                        createQuery(x, age, z, SPARQLFilter.build("?z > 23")),
                        singleton(createQuery(x, age, z, SPARQLFilter.build("?z > 23"))),
                        emptyList()),
                // complete match with  molecule that has input and susuming filter
                asList(person1i2oe1f,
                        createQuery(x, name, y,
                                w, primaryTopic, x,
                                w, title, u,
                                x, age, z, SPARQLFilter.build("?z > 23")),
                        singleton(createQuery(x, name, y,
                                w, primaryTopic, x,
                                w, title, u,
                                x, age, z, SPARQLFilter.build("?z > 23"))),
                        emptyList()),
                // query without filter against simple molecule with filter
                asList(person2oe1f,
                        createQuery(x, age, z),
                        singleton(createQuery(x, age, z)),
                        emptyList()),
                // conjunctive query without filter against simple molecule
                asList(person2oe1f,
                        createQuery(x, name, y,
                                    x, age, z),
                        singleton(createQuery(x, name, y,
                                              x, age, z)),
                        emptyList()),
                // query with incompatible filter
                asList(person2oe1f,
                        createQuery(x, age, z, SPARQLFilter.build("?x < 23")),
                        singleton(createQuery(x, age, z)),
                        emptyList()),
                // conjunctive query with incompatible filter
                asList(person2oe1f,
                        createQuery(x, name, y,
                                    x, age, z, SPARQLFilter.build("?z < 23")),
                        singleton(createQuery(x, name, y,
                                              x, age, z)),
                        emptyList())
        );
    }

    /* --- Multi-core molecules --- */
    public static @Nonnull URI length = new StdURI(EX+"length");

    public static @Nonnull Molecule person_desk = person1oe.toBuilder()
            .startNewCore("Desk").out(length, new Atom("length"))
            .exclusive().closed().build();
    public static @Nonnull Molecule person1i2oeC_desk1i1oe = Molecule.builder("Person1i2oeC")
            .in(primaryTopic, new Atom("Document"))
            .out(name, new Atom("name")).out(age, new Atom("age"))
            .exclusive().nonClosed()
            .startNewCore("Desk1i1oe")
                .in(made, new Atom("made"))
                .out(length, new Atom("length"))
            .exclusive().closed().build();

    private static List<List<Object>> multiCoreMatchData() {
        return asList(
                // these three cases check if all triples are visible
                asList(person_desk, createQuery(x, name, y),
                                    singleton(createQuery(x, name, y)),
                                    emptyList()),
                asList(person_desk, createQuery(x, length, y),
                                    singleton(createQuery(x, length, y)),
                                    emptyList()),
                asList(person_desk, createQuery(x, name, u, y, length, v),
                                    asList(createQuery(x, name, u),
                                           createQuery(y, length, v)),
                                    emptyList()),
                // check if input edges from both cores are visible
                asList(person1i2oeC_desk1i1oe,
                       createQuery(u, primaryTopic, x, x, name,   v,
                                   w, made,         y, y, length, z),
                       asList(createQuery(u, primaryTopic, x, x, name,   v),
                              createQuery(w, made,         y, y, length, z)),
                       emptyList()),
                // use ambiguity (x == w) to match both cores in a path
                asList(person1i2oeC_desk1i1oe,
                       createQuery(u, primaryTopic, x, x, name,   v,
                                   x, made,         y, y, length, z),
                       // two EGs since we cannot prove (at this point) that the join
                       // on x will occur inside this Molecule (the atoms are exclusive,
                       // not the molecule). In practice this modellign is bad modelling,
                       // if the join where possible, then Desk should not be a core but a
                       // child of the Person core
                       asList(createQuery(u, primaryTopic, x, x, name,   v),
                              createQuery(x, made,         y, y, length, z)),
                       emptyList())
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
        list.addAll(exclusiveMatchDataWithFilters());
        list.addAll(multiCoreMatchData());
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

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "matchData")
    public void testMatch(@Nonnull Description description, @Nonnull Object queryOrList,
                          @Nonnull Collection<?> exclusiveGroups,
                          @Nonnull Collection<Triple> nonExclusive) {
        /* treat test inputs */
        CQuery query = queryOrList instanceof CQuery ? (CQuery) queryOrList
                                                     : CQuery.from((List<Triple>)queryOrList);
        List<Collection<Triple>> exclusiveGroupsAsTriples = new ArrayList<>();
        for (Object eg : exclusiveGroups) {
            if (eg instanceof CQuery)
                exclusiveGroupsAsTriples.add(((CQuery)eg).attr().getSet());
            else
                exclusiveGroupsAsTriples.add((Collection<Triple>)eg);
        }

        CQueryMatch match = description.match(query);
        assertEquals(match.getQuery(), query);

        // compare relevant & irrelevant triple patterns
        assertEquals(newHashSet(match.getAllRelevant()), concat(nonExclusive.stream(),
                exclusiveGroupsAsTriples.stream().flatMap(Collection::stream)).collect(toSet()));
        HashSet<Triple> irrelevant = newHashSet(match.getAllRelevant());
        irrelevant.retainAll(match.getIrrelevant());
        assertEquals(irrelevant, emptySet());

        // compare exclusive groups. Ignore ordering
        Set<Set<Triple>> actualGroups = match.getKnownExclusiveGroups().stream()
                .map(Sets::newHashSet).collect(toSet());
        try {
            assertEquals(actualGroups, exclusiveGroupsAsTriples.stream().map(Sets::newHashSet)
                                                      .collect(toSet()));
        } catch (AssertionError e) {
            if (description instanceof MoleculeMatcherWithDisjointness) {
                Set<Triple> set = exclusiveGroupsAsTriples.stream().flatMap(Collection::stream)
                                                          .collect(toSet());
                assertEquals(actualGroups, singleton(set));
            } else {
                throw e;
            }
        }

        if (!(description instanceof MoleculeMatcherWithDisjointness)) {
            for (Object eg : exclusiveGroups) {
                if (!(eg instanceof CQuery)) continue;
                CQuery expected = (CQuery) eg;
                Set<CQuery> matchingGroups = match.getKnownExclusiveGroups().stream()
                        .filter(a -> a.attr().getSet().equals(expected.attr().getSet()))
                        .filter(a -> a.getModifiers().containsAll(expected.getModifiers()))
                        .filter(a -> {
                            boolean[] ok = {true};
                            expected.forEachTermAnnotation((t, ann)
                                    -> ok[0] &= a.getTermAnnotations(t).contains(ann));
                            return ok[0];
                        })
                        .filter(a -> {
                            boolean[] ok = {true};
                            expected.forEachTripleAnnotation((t, ann)
                                    -> ok[0] &= a.getTripleAnnotations(t).contains(ann));
                            return ok[0];
                        }).collect(toSet());
                assertEquals(matchingGroups.size(), 1);
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
                assertEquals(alternative.attr().matchedTriples(), eg.attr().getSet());
            }
        }
    }
}
