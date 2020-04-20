package br.ufsc.lapesd.riefederator.linkedator.strategies.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.query.SimplePath;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.SimplePath.EMPTY;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class AtomSignatureTest implements TestContext {

    private final static Molecule Person;
    private final static Molecule Book;
    private final static APIMolecule PersonByEmail, BookByTitle, BookByGenre;
    private final static AtomSignature PersonByEmailInput, BookByTitleInput,
                                       BookByGenreInput1, BookByGenreInput2;

    static {
        Person = Molecule.builder("Person")
                .out(name, new Atom("name"))
                .out(mbox, new Atom("email"))
                .out(age, new Atom("age"))
                .exclusive().closed().build();
        Book = Molecule.builder("Book")
                .out(author, Person.getCore())
                .in(isAuthorOf, Person.getCore())
                .out(title, new Atom("title"))
                .out(genre, Molecule.builder("Genre")
                        .out(genreName, new Atom("genreName")).buildAtom())
                .exclusive().closed().build();

        Map<String, String> el2in = new HashMap<>();
        HashMap<String, SimplePath> pathsMap = new HashMap<>();
        UriTemplateExecutor executor;

        executor = UriTemplateExecutor.from(new UriTemplate("http://example.org/Person/{?email}"))
                                      .withRequired("email").build();
        el2in.put("email", "email");
        PersonByEmail = new APIMolecule(Person, executor, el2in);
        pathsMap.put("email", SimplePath.to(mbox).build());
        PersonByEmailInput = new AtomSignature(Person, "Person", EMPTY,
                                               singleton("email"), pathsMap);

        el2in.clear();
        pathsMap.clear();
        executor = UriTemplateExecutor.from(new UriTemplate("http://example.org/Books/{?title}"))
                .withRequired("title").build();
        el2in.put("title", "title");
        BookByTitle = new APIMolecule(Book, executor, el2in);
        pathsMap.put("title", SimplePath.to(title).build());
        BookByTitleInput = new AtomSignature(Book, "Book", EMPTY, singleton("title"), pathsMap);

        el2in.clear();
        pathsMap.clear();
        executor = UriTemplateExecutor.from(new UriTemplate("http://example.org/Books/{?genre}"))
                .withRequired("genre").build();
        el2in.put("genreName", "genre");
        BookByGenre = new APIMolecule(Book, executor, el2in);
        pathsMap.put("genreName", SimplePath.to(genre).to(genreName).build());
        BookByGenreInput1 = new AtomSignature(Book, "Book", EMPTY, singleton("genreName"), pathsMap);

        pathsMap.clear();
        pathsMap.put("genreName", SimplePath.to(genreName).build());
        BookByGenreInput2 = new AtomSignature(Book, "Genre", SimplePath.to(genre).build(),
                                              singleton("genreName"), pathsMap);
    }

    @Test
    public void testEquals() {
        HashMap<String, SimplePath> pathsMap = new HashMap<>();
        pathsMap.put("email", SimplePath.to(mbox).build());
        AtomSignature eq = new AtomSignature(Person, "Person", EMPTY,
                                             singleton("email"), pathsMap);
        assertEquals(PersonByEmailInput, eq);

        assertNotEquals(PersonByEmailInput,
                new AtomSignature(Person, "Person", EMPTY, emptySet(), pathsMap));

        pathsMap.put("name", SimplePath.to(name).build());
        assertNotEquals(PersonByEmailInput,
                new AtomSignature(Person, "Person", EMPTY, singleton("email"), pathsMap));

        pathsMap.clear();
        pathsMap.put("name", SimplePath.to(name).build());
        assertNotEquals(PersonByEmailInput,
                new AtomSignature(Person, "Person", EMPTY, singleton("name"), pathsMap));

        pathsMap.clear();
        pathsMap.put("title", SimplePath.to(title).build());
        assertEquals(BookByTitleInput,
                new AtomSignature(Book, "Book", EMPTY, singleton("title"), pathsMap));

        assertNotEquals(BookByTitleInput,
                new AtomSignature(Book, "Author", SimplePath.to(author).build(),
                                  singleton("title"), pathsMap));

        pathsMap.clear();
        pathsMap.put("email", SimplePath.to(mbox).build());
        assertNotEquals(BookByTitleInput,
                new AtomSignature(Book, "Author", SimplePath.to(author).build(),
                                  singleton("email"), pathsMap));
    }

    @DataProvider
    public static Object[][] inputSignatureData() {
        return Stream.of(
                asList(PersonByEmail, singleton(PersonByEmailInput)),
                asList(BookByTitle, singleton(BookByTitleInput)),
                asList(BookByGenre, asList(BookByGenreInput1, BookByGenreInput2))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "inputSignatureData")
    public void testCreateInputSignatures(APIMolecule apiMolecule,
                                          Collection<AtomSignature> expected) {
        Set<AtomSignature> set = AtomSignature.createInputSignatures(apiMolecule);
        assertEquals(set, new HashSet<>(expected));
    }

    @DataProvider
    public @Nonnull Object[][] findPathsData() {
        HashMultimap<String, AtomPath> mm0 = HashMultimap.create();
        mm0.put("Person", AtomPath.EMPTY);

        HashMultimap<String, AtomPath> mm1 = HashMultimap.create();
        mm1.put("email", new AtomPath(asList("Person", "email")));

        HashMultimap<String, AtomPath> mm2 = HashMultimap.create();
        mm2.put("email", new AtomPath(asList("Book", "Person", "email")));

        HashMultimap<String, AtomPath> mm3 = HashMultimap.create();
        mm3.put("title", new AtomPath(asList("Book", "title")));
        mm3.put("genreName", new AtomPath(asList("Book", "Genre", "genreName")));
        mm3.put("email", new AtomPath(asList("Book", "Person", "email")));

        HashMultimap<String, AtomPath> mm4 = HashMultimap.create();
        mm4.put("Genre", new AtomPath(asList("Book", "Genre")));

        return Stream.of(
                asList(Person, singleton("Person"), mm0),
                asList(Person, singleton("email"), mm1),
                asList(Book, singleton("email"), mm2),
                asList(Book, Sets.newHashSet("title", "genreName", "email"), mm3),
                asList(Book, singleton("Genre"), mm4)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "findPathsData")
    public void testFindPathsFromCore(Molecule molecule, Set<String> atomNames,
                                      Multimap<String, AtomPath> expected) {
        Multimap<String, AtomPath> paths = AtomSignature.findPathsFromCore(molecule, atomNames);
        assertEquals(paths, expected);
    }
}