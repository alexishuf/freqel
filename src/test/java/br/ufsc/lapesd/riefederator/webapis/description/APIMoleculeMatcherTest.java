package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.reason.tbox.OWLAPITBoxReasoner.structural;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class APIMoleculeMatcherTest {
    public static final @Nonnull String EX = "http://example.org/";
    public static final @Nonnull URI title = new StdURI(EX+"title");
    public static final @Nonnull URI author = new StdURI(EX+"author");
    public static final @Nonnull URI mainAuthor = new StdURI(EX+"mainAuthor");
    public static final @Nonnull URI bornIn = new StdURI(EX+"bornIn");
    public static final @Nonnull URI name = new StdURI(EX+"name");
    public static final @Nonnull URI cites = new StdURI(EX+"cites");
    public static final @Nonnull URI authorName = new StdURI(EX+"authorName");
    public static final @Nonnull URI author1 = new StdURI(EX+"authors/1");
    public static final @Nonnull URI city1 = new StdURI(EX+"cities/1");
    public static final @Nonnull Lit title1 = StdLit.fromEscaped("title1", "en");
    public static final @Nonnull Lit title2 = StdLit.fromEscaped("title2", "en");
    public static final @Nonnull Lit authorName1 = StdLit.fromEscaped("name1", "en");
    public static final @Nonnull Var X = new StdVar("x");
    public static final @Nonnull Var Y = new StdVar("y");
    public static final @Nonnull Var Z = new StdVar("z");
    public static final @Nonnull Var W = new StdVar("w");

    public static final @Nonnull APIMolecule BOOKS_BY_AUTHOR, BOOK_CITATIONS, AM_BOOK_CITATIONS;
    public static final @Nonnull Atom AUTHOR, AUTHOR_NAME, BOOK_TITLE, CITED_BOOK,
                                      AM_CITED_BOOK, CITED_BOOK_TITLE;

    public static final @Nonnull APIMolecule BOOKS_BY_MAIN_AUTHOR;
    public static final @Nonnull Atom MAIN_AUTHOR;

    static {
        AUTHOR_NAME = Molecule.builder("authorName").buildAtom();
        BOOK_TITLE = Molecule.builder("bookTitle").buildAtom();
        CITED_BOOK_TITLE = Molecule.builder("citedBookTitle").buildAtom();
        AUTHOR = Molecule.builder("Author").out(authorName, AUTHOR_NAME).exclusive().buildAtom();
        Molecule molecule = Molecule.builder("Book")
                .out(title, BOOK_TITLE)
                .out(author, AUTHOR)
                .exclusive()
                .build();
        Map<String, String> atom2var = new HashMap<>();
        atom2var.put("authorName", "hasAuthorName");
        UriTemplateExecutor exec = createExecutor("/books/{?hasAuthorName}");
        BOOKS_BY_AUTHOR = new APIMolecule(molecule, exec, atom2var);

        CITED_BOOK = Molecule.builder("CitedBook")
                .out(title, CITED_BOOK_TITLE)
                .out(author, AUTHOR)
                .exclusive()
                .buildAtom();
        molecule = Molecule.builder("Book")
                .out(title, BOOK_TITLE)
                .out(author, AUTHOR)
                .out(cites, CITED_BOOK)
                .exclusive()
                .build();
        atom2var.clear();
        atom2var.put(BOOK_TITLE.getName(), "hasTitle");
        exec = createExecutor("/books/citations/{?hasTitle}");
        BOOK_CITATIONS = new APIMolecule(molecule, exec, atom2var);

        AM_CITED_BOOK = Molecule.builder("CitedBook")
                .out(title, BOOK_TITLE)
                .out(author, AUTHOR)
                .exclusive()
                .buildAtom();
        molecule = Molecule.builder("Book")
                .out(title, BOOK_TITLE)
                .out(author, AUTHOR)
                .out(cites, AM_CITED_BOOK)
                .exclusive()
                .build();
        atom2var.clear();
        atom2var.put(BOOK_TITLE.getName(), "hasTitle");
        exec = createExecutor("/books/citations/{?hasTitle}");
        AM_BOOK_CITATIONS = new APIMolecule(molecule, exec, atom2var);

        /* ~~~ molecules used in testSemanticMatch() ~~~ */

        MAIN_AUTHOR = Molecule.builder("MainAuthor")
                .out(authorName, AUTHOR_NAME)
                .exclusive().buildAtom();
        molecule = Molecule.builder("Book")
                .out(title, BOOK_TITLE)
                .out(mainAuthor, MAIN_AUTHOR)
                .exclusive().build();
        exec = createExecutor("/books/{?hasMainAuthorName}");
        atom2var.clear();
        atom2var.put(AUTHOR_NAME.getName(), "hasMainAuthorName");
        BOOKS_BY_MAIN_AUTHOR = new APIMolecule(molecule, exec, atom2var);
    }

    private static @Nonnull UriTemplateExecutor createExecutor(String path) {
        return new UriTemplateExecutor(new UriTemplate(EX + path));
    }

    @DataProvider
    public static Object[][] matchData() {
        List<CQuery> e = emptyList();
        return Stream.of(
                asList(BOOKS_BY_AUTHOR, singleton(new Triple(X, author, author1)), e),
                asList(BOOKS_BY_AUTHOR, singleton(new Triple(X, authorName, Y)), e),
                asList(BOOKS_BY_AUTHOR, singleton(new Triple(X, authorName, authorName1)),
                       singleton(CQuery.builder()
                               .add(new Triple(X, authorName, authorName1))
                               .annotate(X, AtomAnnotation.of(AUTHOR))
                               .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                               .build())),
                asList(BOOKS_BY_AUTHOR,
                       asList(new Triple(X, author, Y), new Triple(Y, authorName, authorName1)),
                       singleton(CQuery.builder()
                                .add(new Triple(X, author, Y),
                                     new Triple(Y, authorName, authorName1))
                                .annotate(X, AtomAnnotation.of(BOOKS_BY_AUTHOR.getMolecule().getCore()))
                                .annotate(Y, AtomAnnotation.of(AUTHOR))
                                .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                                .build())),
                asList(BOOKS_BY_AUTHOR,
                       asList(new Triple(X, bornIn, city1),
                              new Triple(X, name, Y),
                              new Triple(Z, author, W),
                              new Triple(W, authorName, Y)),
                       singleton(CQuery.with(asList(new Triple(Z, author, W),
                                                    new Triple(W, authorName, Y)))
                                       .annotate(Z, AtomAnnotation.of(BOOKS_BY_AUTHOR.getMolecule().getCore()))
                                       .annotate(W, AtomAnnotation.of(AUTHOR))
                                       .annotate(Y, AtomAnnotation.asRequired(AUTHOR_NAME))
                                       .build()
                               )),
                asList(BOOK_CITATIONS, singleton(new Triple(X, title, title1)),
                        singleton(CQuery.with(new Triple(X, title, title1))
                                .annotate(X, AtomAnnotation.of(BOOK_CITATIONS.getMolecule().getCore()))
                                .annotate(title1, AtomAnnotation.asRequired(BOOK_TITLE))
                                .build())),
                asList(BOOK_CITATIONS, asList(new Triple(X, title, title1),
                                              new Triple(X, cites, Y),
                                              new Triple(Y, author, author1)),
                        singleton(CQuery.with(new Triple(X, title, title1),
                                              new Triple(X, cites, Y),
                                              new Triple(Y, author, author1))
                                .annotate(X, AtomAnnotation.of(BOOK_CITATIONS.getMolecule().getCore()))
                                .annotate(title1, AtomAnnotation.asRequired(BOOK_TITLE))
                                .annotate(Y, AtomAnnotation.of(CITED_BOOK))
                                .annotate(author1, AtomAnnotation.of(AUTHOR))
                                .build())),
                asList(BOOK_CITATIONS, asList(new Triple(X, title, title1),
                                              new Triple(X, cites, Y),
                                              new Triple(Y, title, Z)),
                        asList(CQuery.with(new Triple(X, title, title1),
                                              new Triple(X, cites, Y),
                                              new Triple(Y, title, Z))
                                .annotate(X, AtomAnnotation.of(BOOK_CITATIONS.getMolecule().getCore()))
                                .annotate(title1, AtomAnnotation.asRequired(BOOK_TITLE))
                                .annotate(Y, AtomAnnotation.of(CITED_BOOK))
                                .annotate(Z, AtomAnnotation.of(CITED_BOOK_TITLE))
                                .build(),
                               CQuery.with(new Triple(Y, title, Z))
                                       .annotate(Y, AtomAnnotation.of(BOOK_CITATIONS.getMolecule().getCore()))
                                       .annotate(Z, AtomAnnotation.asRequired(BOOK_TITLE))
                                       .build())),
                asList(AM_BOOK_CITATIONS, asList(new Triple(X, title, title1),
                                                 new Triple(X, cites, Y),
                                                 new Triple(Y, title, title2)),
                       emptyList()), // ambiguous: no result
                asList(AM_BOOK_CITATIONS, asList(new Triple(X, title, title1),
                                                 new Triple(X, cites, Y),
                                                 new Triple(Y, title, Z)),
                       emptyList()) // ambiguous: no result
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "matchData")
    public void testMatch(APIMolecule apiMolecule, Collection<Triple> query,
                          Collection<CQuery> egs) {
        APIMoleculeMatcher matcher = new APIMoleculeMatcher(apiMolecule, structural());
        CQueryMatch match = matcher.match(CQuery.from(query));
        assertCompatibleKnownExclusiveGroups(match.getKnownExclusiveGroups(), egs);
    }

    private void assertCompatibleKnownExclusiveGroups(Collection<CQuery> actual,
                                                      Collection<CQuery> expected) {
        if (actual == expected)
            return; //same
        assertEquals(actual.size(), expected.size());
        for (CQuery eg : expected) {
            boolean ok = actual.stream().anyMatch(a -> {
                if (!a.getSet().equals(eg.getSet())) return false;
                boolean[] annOk = {true};
                eg.forEachTermAnnotation((term, ann) -> {
                    if (!a.getTermAnnotations(term).contains(ann))
                        annOk[0] = false;
                });
                return annOk[0];
            });
            assertTrue(ok, "Expected EG "+eg+" missing in actual");
        }
    }

    @DataProvider
    public static @Nonnull Object[][] semanticMatchData() {
        List<Object[]> plain = asList(matchData());
        List<Object[]> semantic= Stream.of(
            asList(BOOKS_BY_MAIN_AUTHOR, asList(new Triple(X, mainAuthor, Y),
                                                new Triple(Y, authorName, authorName1)),
                   singleton(CQuery.with(new Triple(X, mainAuthor, Y),
                                         new Triple(Y, authorName, authorName1))
                           .annotate(X, AtomAnnotation.of(BOOKS_BY_MAIN_AUTHOR.getMolecule().getCore()))
                           .annotate(Y, AtomAnnotation.of(MAIN_AUTHOR))
                           .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                           .build())),
            asList(BOOKS_BY_MAIN_AUTHOR, asList(new Triple(X, author, Y),
                                                new Triple(Y, authorName, authorName1)),
                    singleton(CQuery.with(new Triple(X, author, Y),
                                          new Triple(Y, authorName, authorName1))
                            .annotate(X, AtomAnnotation.of(BOOKS_BY_MAIN_AUTHOR.getMolecule().getCore()))
                            .annotate(Y, AtomAnnotation.of(MAIN_AUTHOR))
                            .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                            .build())),
            asList(BOOKS_BY_MAIN_AUTHOR, asList(new Triple(X, title,  Z),
                                                new Triple(X, author, Y),
                                                new Triple(Y, authorName, authorName1)),
                    singleton(CQuery.with(new Triple(X, title,  Z),
                                          new Triple(X, author, Y),
                                          new Triple(Y, authorName, authorName1))
                            .annotate(X, AtomAnnotation.of(BOOKS_BY_MAIN_AUTHOR.getMolecule().getCore()))
                            .annotate(Z, AtomAnnotation.of(BOOK_TITLE))
                            .annotate(Y, AtomAnnotation.of(MAIN_AUTHOR))
                            .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                            .build())),
            asList(BOOKS_BY_MAIN_AUTHOR, asList(new Triple(X, title,  title1),
                                                new Triple(X, author, Y),
                                                new Triple(Y, authorName, authorName1)),
                    singleton(CQuery.with(new Triple(X, title,  title1),
                                          new Triple(X, author, Y),
                                          new Triple(Y, authorName, authorName1))
                            .annotate(X, AtomAnnotation.of(BOOKS_BY_MAIN_AUTHOR.getMolecule().getCore()))
                            .annotate(title1, AtomAnnotation.of(BOOK_TITLE))
                            .annotate(Y, AtomAnnotation.of(MAIN_AUTHOR))
                            .annotate(authorName1, AtomAnnotation.asRequired(AUTHOR_NAME))
                            .build())),
            asList(BOOKS_BY_MAIN_AUTHOR, singleton(new Triple(X, author, author1)),
                   emptyList()),
            asList(BOOKS_BY_MAIN_AUTHOR, singleton(new Triple(X, author, Y)),
                   emptyList())
        ).map(List::toArray).collect(Collectors.toList());
        return Stream.concat(plain.stream(), semantic.stream()).toArray(Object[][]::new);
    }

    @Test(dataProvider = "semanticMatchData")
    public void testSemanticMatch(APIMolecule apiMolecule, Collection<Triple> query,
                                  Collection<CQuery> egs) {
        TransitiveClosureTBoxReasoner reasoner = new TransitiveClosureTBoxReasoner();
        TBoxSpec tboxSpec = new TBoxSpec()
                .addResource(getClass(), "../../api-molecule-matcher-tests.ttl");
        reasoner.load(tboxSpec);

        APIMoleculeMatcher matcher = new APIMoleculeMatcher(apiMolecule, reasoner);
        CQueryMatch match = matcher.semanticMatch(CQuery.from(query));
        assertCompatibleKnownExclusiveGroups(match.getKnownExclusiveGroups(), egs);
    }
}