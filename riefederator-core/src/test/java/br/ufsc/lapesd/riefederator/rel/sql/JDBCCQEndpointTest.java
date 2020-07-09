package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.rel.mappings.impl.ContextMapping;
import br.ufsc.lapesd.riefederator.util.DictTree;
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

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class JDBCCQEndpointTest implements TestContext {
    private static final URI uriAlice   = new StdURI(EX+"inst/1");
    private static final URI uriBob    = new StdURI(EX+"inst/2");
    private static final Lit Alice   = StdLit.fromUnescaped("Alice",   xsdString);
    private static final Lit Bob     = StdLit.fromUnescaped("Bob",     xsdString);
    private static final Lit Charlie = StdLit.fromUnescaped("Charlie", xsdString);
    private static final Lit Dave    = StdLit.fromUnescaped("Dave",    xsdString);
    private static final Lit Eddie   = StdLit.fromUnescaped("Eddie",   xsdString);
    private static final Lit Stanford   = StdLit.fromUnescaped("Stanford",   xsdString);
    private static final Lit MIT   = StdLit.fromUnescaped("MIT",   xsdString);
    private static final Lit i1   = StdLit.fromUnescaped("1",   xsdInt);
    private static final Lit i4   = StdLit.fromUnescaped("4",   xsdInt);
    private static final Lit i5   = StdLit.fromUnescaped("5",   xsdInt);

    @DataProvider
    public static @Nonnull Object[][] queryData() {
        return Stream.of(
                // enumerate a varchar column
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx, u, Projection.required("u")),
                       newHashSet(
                               MapSolution.build(u, Alice),
                               MapSolution.build(u, Bob),
                               MapSolution.build(u, Charlie),
                               MapSolution.build(u, Dave),
                               MapSolution.build(u, Eddie))),
                // Get alice university
                // single star, no joins nor URI generation
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, university, u, Projection.required("u")),
                        singleton(MapSolution.build(u, Stanford))
                ),
                // who is in the same university as alice?
                // one object-object join, project-out subjects
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx,     Alice,
                                   x, university, u,
                                   y, university, u,
                                   y, nameEx,     v, Projection.required("v")),
                       newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Bob))
                ),
                // URI generation: Get Alice URI and id
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice, x, id, v),
                        singleton(MapSolution.builder().put(x, uriAlice).put(v, i1).build())
                ),
                // URI generation: Get Alice URI
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice),
                        singleton(MapSolution.build(x, uriAlice))
                ),
                // URI generation: Get URIs of persons in Stanford
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, university,  Stanford, Projection.required("x")),
                        newHashSet(MapSolution.build(x, uriAlice), MapSolution.build(x, uriBob))
                ),
                // URI gneration: Get URIs and names of persons in Stanford
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, v,  x, university, Stanford),
                        newHashSet(MapSolution.builder().put(x, uriAlice).put(v, Alice).build(),
                                   MapSolution.builder().put(x, uriBob).put(v, Bob).build())
                ),
                // Simple filter: gets ids of two oldest
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, id, v, x, ageEx, u, SPARQLFilter.build("?u >= 25"),
                                    Projection.required("v")),
                        newHashSet(MapSolution.build(v, i4), MapSolution.build(v, i5))
                ),
                // Expression filter (||):  get the oldest and youngest
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, v, x, ageEx, u,
                                    SPARQLFilter.build("?u > 25 || ?u < 23"),
                                    Projection.required("v")),
                        newHashSet(MapSolution.build(v, Alice), MapSolution.build(v, Eddie))
                ),
                // Positive ask query with single star
                asList("dump-1.sql", "sql-mapping-2.json",
                       createQuery(x, nameEx, u,
                                   x, ageEx, v, SPARQLFilter.build("?v > 20"), Ask.REQUIRED),
                       singleton(ArraySolution.EMPTY)),
                // Negative ask query with single star
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx, u,
                                    x, ageEx, v, SPARQLFilter.build("?v < 18"), Ask.REQUIRED),
                        emptySet()),
                // Positive ask with two stars: someone older than Alice in same university?
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o > ?v"), Ask.REQUIRED),
                        singleton(ArraySolution.EMPTY)),
                // Negative ask with two stars: someone **younger** than Alice in same university?
                asList("dump-1.sql", "sql-mapping-2.json",
                        createQuery(x, nameEx,     Alice,
                                    x, ageEx,      v,
                                    x, university, u,
                                    y, university, u,
                                    y, ageEx,      o, SPARQLFilter.build("?o < ?v"), Ask.REQUIRED),
                        emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryData", groups = {"fast"})
    public void testQuery(@Nonnull String dumpPath, @Nonnull String mappingPath,
                          @Nonnull CQuery query,
                          @Nonnull Collection<Solution> expected) throws Exception {
        DictTree mappingDictTree = DictTree.load().fromResource(getClass(), mappingPath);
        ContextMapping mapping = ContextMapping.parse(mappingDictTree);
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
                                @Nonnull Collection<Solution> expected) throws Exception {
        testQuery(dumpPath, mappingPath, query, expected);
    }
}