package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.context.ContextMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class JDBCCQEndpointTest implements TestContext {
    private static final Column University_id = new Column("University", "id");
    private static final Column University_name = new Column("University", "name");
    private static final Column Paper_id = new Column("Paper", "id");
    private static final Column Paper_title = new Column("Paper", "title");
    private static final Column Authorship_paper_id = new Column("Authorship", "paper_id");
    private static final Column Authorship_author_id = new Column("Authorship", "author_id");
    private static final Column Person_id = new Column("Person", "id");
    private static final Column Person_name = new Column("Person", "name");
    private static final Column Person_age = new Column("Person", "age");
    private static final Column Person_university_id = new Column("Person", "university_id");
    private static final Column Person_supervisor = new Column("Person", "supervisor");

    private static final Atom aUniversity_id = Molecule.builder("University.id")
            .tag(ColumnsTag.direct(University_id)).buildAtom();
    private static final Atom aUniversity_name = Molecule.builder("University.name")
            .tag(ColumnsTag.direct(University_name)).buildAtom();
    private static final Atom aUniversity = Molecule.builder("University")
            .out(idEx, aUniversity_id, ColumnsTag.direct(University_id))
            .out(nameEx, aUniversity_name, ColumnsTag.direct(University_name))
            .tag(new TableTag("University")).exclusive().buildAtom();

    private static final Atom aPaper_id = Molecule.builder("Paper.id")
            .tag(ColumnsTag.direct(Paper_id)).buildAtom();
    private static final Atom aPaper_title = Molecule.builder("Paper.title")
            .tag(ColumnsTag.direct(Paper_title)).buildAtom();
    private static final Atom aPaper = Molecule.builder("Paper")
            .out(idEx, aPaper_id, ColumnsTag.direct(Paper_id))
            .out(titleEx, aPaper_title, ColumnsTag.direct(Paper_title))
            .tag(new TableTag("Paper")).exclusive().buildAtom();


    private static final Atom aPerson_id = Molecule.builder("Person.id")
            .tag(ColumnsTag.direct(Person_id)).buildAtom();
    private static final Atom aPerson_name = Molecule.builder("Person.name")
            .tag(ColumnsTag.direct(Person_name)).buildAtom();
    private static final Atom aPerson_age = Molecule.builder("Person.age")
            .tag(ColumnsTag.direct(Person_age)).buildAtom();
    private static final Atom aPerson_university_id = Molecule.builder("Person.university_id")
            .tag(ColumnsTag.direct(Person_university_id)).buildAtom();
    private static final Atom aPerson = Molecule.builder("Person")
            .out(idEx, aPerson_id, ColumnsTag.direct(Person_id))
            .out(nameEx, aPerson_name, ColumnsTag.direct(Person_name))
            .out(ageEx, aPerson_age, ColumnsTag.direct(Person_age))
            .out(university_id, aPerson_university_id, ColumnsTag.direct(Person_university_id))
            .tag(new TableTag("Person")).exclusive().buildAtom();


    private static final Atom aAuthorship_paper_id = Molecule.builder("Authorship.paper_id")
            .tag(ColumnsTag.direct(Authorship_paper_id)).buildAtom();
    private static final Atom aAuthorship_author_id = Molecule.builder("Authorship.author_id")
            .tag(ColumnsTag.direct(Authorship_author_id)).buildAtom();
    private static final Atom aAuthorship = Molecule.builder("Authorship")
            .out(paper_id, aAuthorship_paper_id, ColumnsTag.direct(Authorship_paper_id))
            .out(author_id, aAuthorship_author_id, ColumnsTag.direct(Authorship_author_id))
            .tag(new TableTag("Authorship")).exclusive().buildAtom();

    private static final AtomAnnotation aaUniversity = AtomAnnotation.of(aUniversity);
    private static final AtomAnnotation aaUniversity_id = AtomAnnotation.of(aUniversity_id);
    private static final AtomAnnotation aaUniversity_name = AtomAnnotation.of(aUniversity_name);

    private static final AtomAnnotation aaPaper = AtomAnnotation.of(aPaper);
    private static final AtomAnnotation aaPaper_id = AtomAnnotation.of(aPaper_id);
    private static final AtomAnnotation aaPaper_title = AtomAnnotation.of(aPaper_title);

    private static final AtomAnnotation aaPerson = AtomAnnotation.of(aPerson);
    private static final AtomAnnotation aaPerson_id = AtomAnnotation.of(aPerson_id);
    private static final AtomAnnotation aaPerson_name = AtomAnnotation.of(aPerson_name);
    private static final AtomAnnotation aaPerson_age = AtomAnnotation.of(aPerson_age);
    private static final AtomAnnotation aaPerson_university_id = AtomAnnotation.of(aPerson_university_id);

    private static final AtomAnnotation aaAuthorship = AtomAnnotation.of(aAuthorship);
    private static final AtomAnnotation aaAuthorship_author_id = AtomAnnotation.of(aAuthorship_author_id);
    private static final AtomAnnotation aaAuthorship_paper_id = AtomAnnotation.of(aAuthorship_paper_id);


    private static final URI uriAlice   = new StdURI(EX+"inst/1");
    private static final URI uriBob    = new StdURI(EX+"inst/2");
    private static final URI uriAuthorship_1_1    = new StdURI(EX+"Authorship/1-1");
    private static final URI uriAuthorship_1_2    = new StdURI(EX+"Authorship/1-2");

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


    @DataProvider
    public static @Nonnull Object[][] queryData() {
        return Stream.of(
                // enumerate a varchar column, do not inspect subjt
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx, u, Projection.required("u")),
                       newHashSet(
                               MapSolution.build(u, Alice),
                               MapSolution.build(u, Bob),
                               MapSolution.build(u, Charlie),
                               MapSolution.build(u, Dave),
                               MapSolution.build(u, Eddie)), false),
                // enumerate a varchar column and generate subjects
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, u),
                        newHashSet(
                                MapSolution.builder().put(x, new StdURI(EX+"inst/1"))
                                        .put(u, Alice).build(),
                                MapSolution.builder().put(x, new StdURI(EX+"inst/2"))
                                        .put(u, Bob).build(),
                                MapSolution.builder().put(x, new StdURI(EX+"inst/3"))
                                        .put(u, Charlie).build(),
                                MapSolution.builder().put(x, new StdURI(EX+"inst/4"))
                                        .put(u, Dave).build(),
                                MapSolution.builder().put(x, new StdURI(EX+"inst/5"))
                                        .put(u, Eddie).build()),
                        false),
                // enumerate subjects that have a varchar column
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, u, Projection.required("x")),
                        newHashSet(
                                MapSolution.build(x, new StdURI(EX+"inst/1")),
                                MapSolution.build(x, new StdURI(EX+"inst/2")),
                                MapSolution.build(x, new StdURI(EX+"inst/3")),
                                MapSolution.build(x, new StdURI(EX+"inst/4")),
                                MapSolution.build(x, new StdURI(EX+"inst/5"))),
                        false),
                // enumerate subjects that have a varchar column, but use a blank node
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, new StdBlank()),
                        newHashSet(
                                MapSolution.build(x, new StdURI(EX+"inst/1")),
                                MapSolution.build(x, new StdURI(EX+"inst/2")),
                                MapSolution.build(x, new StdURI(EX+"inst/3")),
                                MapSolution.build(x, new StdURI(EX+"inst/4")),
                                MapSolution.build(x, new StdURI(EX+"inst/5"))),
                        false),
                // Get alice university
                // single star, no joins nor URI generation
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, university, u, Projection.required("u")),
                        singleton(MapSolution.build(u, Stanford)), false
                ),
                // who is in the same university as alice?
                // one object-object join, project-out subjects
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx,     Alice,
                                   x, university, u,
                                   y, university, u,
                                   y, nameEx,     v, Projection.required("v")),
                       newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Bob)), false
                ),
                // URI generation: Get Alice URI and id
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice, x, idEx, v),
                        singleton(MapSolution.builder().put(x, uriAlice).put(v, i1).build()), false
                ),
                // URI generation: Get Alice URI
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice),
                        singleton(MapSolution.build(x, uriAlice)), false
                ),
                // URI generation: Get URIs of persons in Stanford
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, university,  Stanford, Projection.required("x")),
                        newHashSet(MapSolution.build(x, uriAlice), MapSolution.build(x, uriBob)),
                        false
                ),
                // URI gneration: Get URIs and names of persons in Stanford
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, v,  x, university, Stanford),
                        newHashSet(MapSolution.builder().put(x, uriAlice).put(v, Alice).build(),
                                   MapSolution.builder().put(x, uriBob).put(v, Bob).build()),
                        false
                ),
                // Simple filter: gets ids of two oldest
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, idEx, v, x, ageEx, u, SPARQLFilter.build("?u >= 25"),
                                    Projection.required("v")),
                        newHashSet(MapSolution.build(v, i4), MapSolution.build(v, i5)), false
                ),
                // Expression filter (||):  get the oldest and youngest
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, v, x, ageEx, u,
                                    SPARQLFilter.build("?u > 25 || ?u < 23"),
                                    Projection.required("v")),
                        newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Eddie)), false
                ),
                // Positive ask query with single star
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx, u,
                                   x, ageEx, v, SPARQLFilter.build("?v > 20"), Ask.REQUIRED),
                       singleton(ArraySolution.EMPTY), false),
                // Negative ask query with single star
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, u,
                                    x, ageEx, v, SPARQLFilter.build("?v < 18"), Ask.REQUIRED),
                        emptySet(), false),
                // Positive ask with two stars: someone older than Alice in same university?
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o > ?v"), Ask.REQUIRED),
                        singleton(ArraySolution.EMPTY), false),
                // Negative ask with two stars: someone **younger** than Alice in same university?
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o < ?v"), Ask.REQUIRED),
                        emptySet(), false),
                // Single star on a multi-table database (test dump-2.sql and sql-mapping-2.json)
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, nameEx, Alice, x, ageEx, v, Projection.required("v")),
                        singleton(MapSolution.build(v, i22)), false),
                // Two tables have the predicates: University and Person
                // this leads to an ambiguity: both Person and University satisfy this
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, idEx, v,
                                    x, nameEx, MIT, Projection.required("v")),
                        singleton(MapSolution.build(v, i2)), false),
                // Explore a near case of the previous ambiguity. University matches
                // but is eliminated during planning
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, idEx, v,
                                    x, nameEx, Alice,
                                    x, ageEx, u, Projection.required("v", "u")),
                        singleton(MapSolution.builder().put(v, i1).put(u, i22).build()), false),
                // A simple match on Paper (more of a self-test)
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, idEx, v,
                                    x, titleEx, title2, Projection.required("v")),
                        singleton(MapSolution.build(v, i2)), false),
                // A simple match on Paper (more of a self-test). **already annotated**
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, aaPaper, idEx, v, aaPaper_id,
                                    x, aaPaper, titleEx, title2, aaPaper_title,
                                    Projection.required("v")),
                        singleton(MapSolution.build(v, i2)), false),
                // A simple match on Authorship (more of a self-test)
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, paper_id, i1,
                                    x, author_id, v, Projection.required("v")),
                        newHashSet(MapSolution.build(v, i1), MapSolution.build(v, i2)), false),
                // A simple match on Authorship (more of a self-test) **annotated**
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, aaAuthorship,  paper_id, i1, aaAuthorship_paper_id,
                                    x, aaAuthorship, author_id, v, aaAuthorship_author_id,
                                    Projection.required("v")),
                        newHashSet(MapSolution.build(v, i1), MapSolution.build(v, i2)), false),
                // Composite key URI generation
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, paper_id, i1,
                                    x, author_id, v),
                        newHashSet(MapSolution.builder().put(x, uriAuthorship_1_1)
                                                        .put(v, i1).build(),
                                   MapSolution.builder().put(x, uriAuthorship_1_2)
                                                        .put(v, i2).build()), false),
                // Path-style 3-table join. Title of papers authored by Charlie
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, idEx,      u,
                                    x, nameEx,    Charlie,
                                    y, author_id, u,
                                    y, paper_id,  v,
                                    z, idEx,      v,
                                    z, titleEx,   o, Projection.required("o")),
                        singleton(MapSolution.build(o, title2)), true),
                // The mediator decomposes the above query. Avoid decomposition:
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, aaPerson,     idEx,      u,       aaPerson_id,
                                    x, aaPerson,     nameEx,    Charlie, aaPerson_name,
                                    y, aaAuthorship, author_id, u,       aaAuthorship_author_id,
                                    y, aaAuthorship, paper_id,  v,       aaAuthorship_paper_id,
                                    z, aaPaper,      idEx,      v,       aaPaper_id,
                                    z, aaPaper,      titleEx,   o,       aaPaper_title,
                                    Projection.required("o")),
                        singleton(MapSolution.build(o, title2)), false),
                // Path-style 3-table join. Title of papers authored by who has >=24 years
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, idEx,   u,
                                    x, ageEx,    w, SPARQLFilter.build("?w >= 24"),
                                    y, author_id, u,
                                    y, paper_id,  v,
                                    z, idEx,      v,
                                    z, titleEx,   o, Projection.required("o")),
                        singleton(MapSolution.build(o, title2)), true),
                // **ANNOTATED** Path-style 3-table join. Title of papers authored by who has >=24 years
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x, aaPerson,     idEx,      u, aaPerson_id,
                                    x, aaPerson,     ageEx,     w, aaPerson_age,
                                    SPARQLFilter.build("?w >= 24"),
                                    y, aaAuthorship, author_id, u, aaAuthorship_author_id,
                                    y, aaAuthorship, paper_id,  v, aaAuthorship_paper_id,
                                    z, aaPaper,      idEx,      v, aaPaper_id,
                                    z, aaPaper,      titleEx,   o, aaPaper_title,
                                    Projection.required("o")),
                        singleton(MapSolution.build(o, title2)), false),
                // Join all 4 tables: get papers with authors from both universities
                asList("dump-2.sql", "sql-mapping-3.json",
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
                                    Projection.required("p")),
                        singleton(MapSolution.build(p, title2)), true),
                // **ANNOTATED** Join all 4 tables: get papers with authors from both universities
                asList("dump-2.sql", "sql-mapping-3.json",
                        createQuery(x1, aaPerson,     idEx,           u1,       aaPerson_id,
                                    x1, aaPerson,     university_id,  v1,       aaPerson_university_id,
                                    y1, aaUniversity, idEx,           v1,       aaUniversity_id,
                                    y1, aaUniversity, nameEx,         MIT,      aaUniversity_name,
                                    x2, aaPerson,     idEx,           u2,       aaPerson_id,
                                    x2, aaPerson,     university_id,  v2,       aaPerson_university_id,
                                    y2, aaUniversity, idEx,           v2,       aaUniversity_id,
                                    y2, aaUniversity, nameEx,         Stanford, aaUniversity_name,
                                    z1, aaAuthorship, author_id,      u1,       aaAuthorship_author_id,
                                    z1, aaAuthorship, paper_id,       s,        aaAuthorship_paper_id,
                                    z2, aaAuthorship, author_id,      u2,       aaAuthorship_author_id,
                                    z2, aaAuthorship, paper_id,       s,        aaAuthorship_paper_id,
                                    w,  aaPaper,      idEx,           s,        aaPaper_id,
                                    w,  aaPaper,      titleEx,        p,        aaPaper_title,
                                Projection.required("p")),
                        singleton(MapSolution.build(p, title2)), false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryData", groups = {"fast"})
    public void testQuery(@Nonnull String dumpPath, @Nonnull String mappingPath,
                          @Nonnull CQuery query,
                          @Nonnull Collection<Solution> expected,
                          boolean allowDecomposition) throws Exception {
        List<DictTree> mappingDictTrees = DictTree.load().fromResourceList(getClass(), mappingPath);
        ContextMapping mapping = ContextMapping.parse(mappingDictTrees);
        String url = "jdbc:h2:mem:"+UUID.randomUUID().toString();
        try (Connection connection = DriverManager.getConnection(url)) {
            try (Statement stmt = connection.createStatement();
                 InputStream in = getClass().getResourceAsStream(dumpPath)) {
                assertNotNull(in);
                stmt.executeUpdate(IOUtils.toString(in, StandardCharsets.UTF_8));
            }

            //although MoleculeMatcher breaks queries apart correctly, StandardDecomposer should
            //glue them back together since this is the only source
            boolean[] decomposed = {false};

            JDBCCQEndpoint ep = new JDBCCQEndpoint(mapping, dumpPath,
                    () -> DriverManager.getConnection(url)) {
                @Override
                public @Nonnull Results query(@Nonnull CQuery epQuery) {
                    if (epQuery.size() < query.size())
                        decomposed[0] = true;
                    return super.query(epQuery);
                }
            };

            Set<Solution> actual = new HashSet<>();
            ep.query(query).forEachRemainingThenClose(actual::add);
            assertEquals(actual, new HashSet<>(expected));
            if (!allowDecomposition)
                assertFalse(decomposed[0]);
        }
    }

    /**
     * Try to catch heisenbugs due to non-deterministic scheduling.
     * The multi-threaded invocations share no writable data.
     */
    @Test(dataProvider = "queryData", invocationCount = 4, threadPoolSize = 4)
    public void testQueryRepeat(@Nonnull String dumpPath, @Nonnull String mappingPath,
                                @Nonnull CQuery query,
                                @Nonnull Collection<Solution> expected,
                                boolean allowDecomposition) throws Exception {
        testQuery(dumpPath, mappingPath, query, expected, allowDecomposition);
    }
}