package br.ufsc.lapesd.freqel.rel.cql;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerTest;
import br.ufsc.lapesd.freqel.model.term.Blank;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.std.StdBlank;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.webapis.description.AtomAnnotation;
import com.datastax.oss.driver.api.core.CqlSession;
import org.testng.annotations.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.model.term.std.StdBlank.blank;
import static br.ufsc.lapesd.freqel.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;

public class CassandraCQEndpointTest implements TestContext {
    private CassandraHelper cassandra;
    private CassandraCQEndpoint ep;

    private static final @Nonnull Blank referenceBlank = new StdBlank();

    private static final Lit Alice   = fromUnescaped("Alice",   xsdString);
    private static final Lit Bob     = fromUnescaped("Bob",     xsdString);
    private static final Lit Charlie = fromUnescaped("Charlie", xsdString);
    private static final Lit Dave    = fromUnescaped("Dave",    xsdString);
    private static final Lit Eddie   = fromUnescaped("Eddie",   xsdString);

    private static final Lit Stanford   = fromUnescaped("Stanford",   xsdString);
    private static final Lit MIT   = fromUnescaped("MIT",   xsdString);

    private static final Lit i1   = fromUnescaped("1",   xsdInt);
    private static final Lit i2   = fromUnescaped("2",   xsdInt);
    private static final Lit i4   = fromUnescaped("4",   xsdInt);
    private static final Lit i5   = fromUnescaped("5",   xsdInt);
    private static final Lit i22  = fromUnescaped("22",   xsdInt);

    private static final Lit title2   = fromUnescaped("ABC considered harmful", xsdString);

    private static final Column cOtherPerson_id = new Column("other_ks.otherperson", "id");
    private static final Column cOtherPerson_name = new Column("other_ks.otherperson", "name");
    private static final Column cOtherPerson_age = new Column("other_ks.otherperson", "age");
    private static final Column cOtherPerson_university = new Column("other_ks.otherperson", "university");

    private static final Atom OtherPerson_id = Molecule.builder("other_ks.otherperson.id")
            .tag(ColumnsTag.direct(cOtherPerson_id)).buildAtom();
    private static final Atom OtherPerson_name = Molecule.builder("other_ks.otherperson.name")
            .tag(ColumnsTag.direct(cOtherPerson_name)).buildAtom();
    private static final Atom OtherPerson_age = Molecule.builder("other_ks.otherperson.age")
            .tag(ColumnsTag.direct(cOtherPerson_age)).buildAtom();
    private static final Atom OtherPerson_university = Molecule.builder("other_ks.otherperson.university")
            .tag(ColumnsTag.direct(cOtherPerson_university)).buildAtom();
    private static final Atom aOtherPerson = Molecule.builder("other_ks.otherperson")
            .out(idEx, OtherPerson_id, ColumnsTag.direct(cOtherPerson_id))
            .out(nameEx, OtherPerson_name, ColumnsTag.direct(cOtherPerson_name))
            .out(ageEx, OtherPerson_age, ColumnsTag.direct(cOtherPerson_age))
            .out(university, OtherPerson_university, ColumnsTag.direct(cOtherPerson_university))
            .tag(new TableTag("other_ks.otherperson"))
            .exclusive().buildAtom();

    private static final AtomAnnotation aaOtherPerson_name = AtomAnnotation.of(OtherPerson_name);
    private static final AtomAnnotation aaOtherPerson_university = AtomAnnotation.of(OtherPerson_university);
    private static final AtomAnnotation aaOtherPerson = AtomAnnotation.of(aOtherPerson);

    private static final Column cUniversity_id = new Column("def_ks.university", "id");
    private static final Column cUniversity_name = new Column("def_ks.university", "name");
    private static final Column cPaper_id = new Column("def_ks.paper", "id");
    private static final Column cPaper_title = new Column("def_ks.paper", "title");
    private static final Column cPerson_id = new Column("def_ks.person", "id");
    private static final Column cPerson_name = new Column("def_ks.person", "name");
    private static final Column cPerson_age = new Column("def_ks.person", "age");
    private static final Column cPerson_university_id = new Column("def_ks.person", "university_id");
    private static final Column cPerson_supervisor = new Column("def_ks.person", "supervisor");
    private static final Column cAuthorship_paper_id = new Column("def_ks.authorship", "paper_id");
    private static final Column cAuthorship_author_id = new Column("def_ks.authorship", "author_id");

    private static final Atom University_id = Molecule.builder("def_ks.university.id")
            .tag(ColumnsTag.direct(cUniversity_id)).buildAtom();
    private static final Atom University_name = Molecule.builder("def_ks.university.name")
            .tag(ColumnsTag.direct(cUniversity_name)).buildAtom();
    private static final Atom aUniversity = Molecule.builder("def_ks.university")
            .out(idEx, University_id, ColumnsTag.direct(cUniversity_id))
            .out(nameEx, University_name, ColumnsTag.direct(cUniversity_name))
            .tag(new TableTag("def_ks.university"))
            .exclusive().buildAtom();

    private static final Atom Paper_id = Molecule.builder("def_ks.paper.id")
            .tag(ColumnsTag.direct(cPaper_id)).buildAtom();
    private static final Atom Paper_title = Molecule.builder("def_ks.paper.title")
            .tag(ColumnsTag.direct(cPaper_title)).buildAtom();
    private static final Atom aPaper = Molecule.builder("def_ks.paper")
            .out(idEx, Paper_id, ColumnsTag.direct(cPaper_id))
            .out(titleEx, Paper_title, ColumnsTag.direct(cPaper_title))
            .tag(new TableTag("def_ks.paper"))
            .exclusive().buildAtom();

    private static final Atom Person_id = Molecule.builder("def_ks.person.id")
            .tag(ColumnsTag.direct(cPerson_id)).buildAtom();
    private static final Atom Person_name = Molecule.builder("def_ks.person.name")
            .tag(ColumnsTag.direct(cPerson_name)).buildAtom();
    private static final Atom Person_age = Molecule.builder("def_ks.person.age")
            .tag(ColumnsTag.direct(cPerson_age)).buildAtom();
    private static final Atom Person_university_id = Molecule.builder("def_ks.person.university_id")
            .tag(ColumnsTag.direct(cPerson_university_id)).buildAtom();
    private static final Atom Person_supervisor = Molecule.builder("def_ks.person.supervisor")
            .tag(ColumnsTag.direct(cPerson_supervisor)).buildAtom();
    private static final Atom aPerson = Molecule.builder("def_ks.person")
            .out(idEx, Person_id, ColumnsTag.direct(cPerson_id))
            .out(nameEx, Person_name, ColumnsTag.direct(cPerson_name))
            .out(ageEx, Person_age, ColumnsTag.direct(cPerson_age))
            .out(university_id, Person_university_id, ColumnsTag.direct(cPerson_university_id))
            .out(supervisor, Person_supervisor, ColumnsTag.direct(cPerson_supervisor))
            .tag(new TableTag("def_ks.person"))
            .exclusive().buildAtom();

    private static final Atom Authorship_paper_id = Molecule.builder("def_ks.authorship.paper_id")
            .tag(ColumnsTag.direct(cAuthorship_paper_id)).buildAtom();
    private static final Atom Authorship_author_id = Molecule.builder("def_ks.authorship.author_id")
            .tag(ColumnsTag.direct(cAuthorship_author_id)).buildAtom();
    private static final Atom aAuthorship = Molecule.builder("def_ks.authorship")
            .out(paper_id, Authorship_paper_id, ColumnsTag.direct(cAuthorship_paper_id))
            .out(author_id, Authorship_author_id, ColumnsTag.direct(cAuthorship_author_id))
            .tag(new TableTag("def_ks.authorship"))
            .exclusive().buildAtom();

    @BeforeClass
    public void setUp() throws IOException, InterruptedException {
        cassandra = CassandraHelper.createCassandra();
        cassandra.executeCommands("dump-1.cql", getClass());
        cassandra.executeCommands("dump-2.cql", getClass());
    }

    @AfterClass
    public void tearDown() throws Exception {
        cassandra.close();
    }

    @AfterMethod
    public void methodTearDown() {
        if (ep != null) {
            ep.close();
            ep = null;
        }
    }

    @DataProvider
    public static Object[][] getSchemaData() {
        return Stream.of(
                asList("other_ks", Molecule.builder(aOtherPerson).build()),
                asList(null, Molecule.builder(aOtherPerson)
                        .startNewCore(aPerson)
                        .startNewCore(aUniversity)
                        .startNewCore(aPaper)
                        .startNewCore(aAuthorship)
                        .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "getSchemaData")
    public void testGetSchema(@Nullable String keyspace, @Nonnull Molecule expected) {
        CqlSession session = cassandra.getSession();
        if (keyspace == null)
            ep = CassandraCQEndpoint.builder().sharing(session).setPrefix(EX).build();
        else
            ep = CassandraCQEndpoint.builder().sharing(session).setPrefix(EX).build(keyspace);
        Molecule actual = ep.getMolecule();
        assertEquals(actual, expected);
    }

    private Set<Solution> ignoreBlanks(@Nonnull Collection<Solution> collection) {
        Set<Solution> set = new HashSet<>();
        for (Solution solution : collection) {
            MapSolution.Builder b = MapSolution.builder();
            solution.forEach((var, term) -> {
                if (term.isBlank())
                    b.put(var, referenceBlank);
                else
                    b.put(var, term);
            });
            set.add(b.build());
        }
        return set;
    }

    @DataProvider
    public static @Nonnull Object[][] queryData() {
        return Stream.of(
                // enumerate a column, do not inspect subject
                asList("other_ks", createQuery(
                                x, aaOtherPerson, nameEx, u, aaOtherPerson_name,
                                Projection.of("u")),
                        newHashSet(
                                MapSolution.build(u, Alice),
                                MapSolution.build(u, Bob),
                                MapSolution.build(u, Charlie),
                                MapSolution.build(u, Dave),
                                MapSolution.build(u, Eddie))),
                // enumerate a column, generating subjects
                asList("other_ks", createQuery(
                        x, aaOtherPerson, nameEx, u, aaOtherPerson_name),
                        newHashSet(
                                MapSolution.builder().put(x, blank()).put(u, Alice).build(),
                                MapSolution.builder().put(x, blank()).put(u, Bob).build(),
                                MapSolution.builder().put(x, blank()).put(u, Charlie).build(),
                                MapSolution.builder().put(x, blank()).put(u, Dave).build(),
                                MapSolution.builder().put(x, blank()).put(u, Eddie).build())),
                // Get alice university
                // single star, no joins nor URI generation
                asList("other_ks",
                        createQuery(x, aaOtherPerson, nameEx,     Alice, aaOtherPerson_name,
                                    x, aaOtherPerson, university, u, aaOtherPerson_university,
                                   Projection.of("u")),
                        singleton(MapSolution.build(u, Stanford))),
                // who is in the same university as alice?
                // one object-object join, project-out subjects
                asList("other_ks",
                        createQuery(x, nameEx,     Alice,
                                    x, university, u,
                                    y, university, u,
                                    y, nameEx,     v,
                                    Projection.of("v")),
                        newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Bob))),
                // same as above, but annotated
                // this cannot be done in a single CQL query and is broken down
                asList("other_ks",
                        createQuery(x, aaOtherPerson, nameEx,     Alice, aaOtherPerson_name,
                                    x, aaOtherPerson, university, u, aaOtherPerson_university,
                                    y, aaOtherPerson, university, u, aaOtherPerson_university,
                                    y, aaOtherPerson, nameEx,     v, aaOtherPerson_name,
                                    Projection.of("v")),
                        newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Bob))),
                // Simple filter: gets ids of two oldest
                asList("other_ks",
                        createQuery(x, idEx, v,
                                    x, ageEx, u, SPARQLFilter.build("?u >= 25"),
                                    Projection.of("v")),
                        newHashSet(MapSolution.build(v, i4), MapSolution.build(v, i5))),
                // Expression filter (||):  get the oldest and youngest
                // || is not supported by cassandra, so the filter is executed by the mediator
                asList("other_ks",
                        createQuery(x, nameEx, v,
                                    x, ageEx, u,
                                    SPARQLFilter.build("?u > 25 || ?u < 23"),
                                    Projection.of("v")),
                        newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Eddie))),
                // Expression filter (&&):  get the oldest
                // This filter is foolish but gets pushed to cassandra, unlike the previous
                asList("other_ks",
                        createQuery(x, nameEx, v,
                                    x, ageEx, u,
                                    SPARQLFilter.build("?u > 25 && ?u > 23"),
                                    Projection.of("v")),
                        singleton(MapSolution.build(v, Eddie))),
                // Positive ask query with single star
                asList("other_ks",
                        createQuery(x, nameEx, u,
                                    x, ageEx, v, SPARQLFilter.build("?v > 20"), Ask.INSTANCE),
                        singleton(ArraySolution.EMPTY)),
                // Negative ask query with single star
                asList("other_ks",
                        createQuery(x, nameEx, u,
                                    x, ageEx, v, SPARQLFilter.build("?v < 18"), Ask.INSTANCE),
                        emptySet()),
                // Positive ask with two stars: someone older than Alice in same university?
                asList("other_ks",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o > ?v"), Ask.INSTANCE),
                        singleton(ArraySolution.EMPTY)),
                // Negative ask with two stars: someone **younger** than Alice in same university?
                asList("other_ks",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o < ?v"), Ask.INSTANCE),
                        emptySet()),
                // Single star on a multi-table keyspace
                asList("def_ks",
                        createQuery(x, nameEx, Alice,
                                    x, ageEx, v, Projection.of("v")),
                        singleton(MapSolution.build(v, i22))),
                // Two tables have the predicates: University and Person
                // this leads to an ambiguity: both Person and University satisfy this
                asList("def_ks",
                        createQuery(x, idEx, v,
                                    x, nameEx, MIT, Projection.of("v")),
                        singleton(MapSolution.build(v, i2))),
                // Explore a near case of the previous ambiguity. University matches
                // but is eliminated during planning
                asList("def_ks",
                        createQuery(x, idEx, v,
                                    x, nameEx, Alice,
                                    x, ageEx, u, Projection.of("v", "u")),
                        singleton(MapSolution.builder().put(v, i1).put(u, i22).build())),
                // A simple match on Paper (more of a self-test)
                asList("def_ks",
                        createQuery(x, idEx, v,
                                    x, titleEx, title2, Projection.of("v")),
                        singleton(MapSolution.build(v, i2))),
                // A simple match on Authorship (more of a self-test)
                asList("def_ks",
                        createQuery(x, paper_id, i1,
                                    x, author_id, v, Projection.of("v")),
                        newHashSet(MapSolution.build(v, i1), MapSolution.build(v, i2))),
                // Path-style 3-table join. Title of papers authored by Charlie
                asList("def_ks",
                        createQuery(x, idEx,      u,
                                    x, nameEx,    Charlie,
                                    y, author_id, u,
                                    y, paper_id,  v,
                                    z, idEx,      v,
                                    z, titleEx,   o, Projection.of("o")),
                        singleton(MapSolution.build(o, title2))),
                // regression test: this becomes a CQL ask, but is not an SPARQL ask
                asList("other_ks",
                        createQuery(x, idEx, i1,
                                    x, university, Stanford),
                        singleton(MapSolution.build(x, blank()))), //for Alice
                // Path-style 3-table join. Title of papers authored by who has >=24 years
                asList("def_ks",
                        createQuery(x, idEx,   u,
                                    x, ageEx,    w, SPARQLFilter.build("?w >= 24"),
                                    y, author_id, u,
                                    y, paper_id,  v,
                                    z, idEx,      v,
                                    z, titleEx,   o, Projection.of("o")),
                        singleton(MapSolution.build(o, title2))),
                // Join all 4 tables: get papers with authors from both universities
                asList("def_ks",
                        createQuery(x1, idEx,           u1,
                                    x1, university_id,  v1,
                                    y1, idEx,           v1,
                                    y1, nameEx,         MIT,
                                    x2, idEx,           u2,
                                    x2, university_id,  v2,
                                    y2, idEx,           v2,
                                    y2, nameEx,         Stanford,
                                    z1, author_id,      u1,
                                    z1, paper_id,       s,
                                    z2, author_id,      u2,
                                    z2, paper_id,       s,
                                    w, idEx,            s,
                                    w, titleEx,         p,
                                    Projection.of("p")),
                        singleton(MapSolution.build(p, title2)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull String keyspace, @Nonnull CQuery query,
                          @Nonnull Collection<Solution> expected) {
        ep = CassandraCQEndpoint.builder().sharing(cassandra.getSession()).setPrefix(EX)
                                .build(keyspace);
        Set<Solution> actual = new HashSet<>();
        BitsetConjunctivePlannerTest.addUniverseSets(query);
        ep.query(query).forEachRemainingThenClose(actual::add);
        assertEquals(ignoreBlanks(actual), ignoreBlanks(expected));
    }
}