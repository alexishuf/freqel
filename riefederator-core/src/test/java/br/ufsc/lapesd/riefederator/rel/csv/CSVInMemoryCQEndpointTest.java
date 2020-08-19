package br.ufsc.lapesd.riefederator.rel.csv;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.rel.mappings.context.ContextMapping;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = {"fast"})
public class CSVInMemoryCQEndpointTest implements TestContext {

    @DataProvider
    public static Object[][] queryData() {
        StdLit alice = StdLit.fromUnescaped("Alice", xsdString);
        StdLit bob = StdLit.fromUnescaped("Bob", xsdString);
        StdLit charlie = StdLit.fromUnescaped("Charlie", xsdString);
        StdLit dave = StdLit.fromUnescaped("Dave", xsdString);
        StdLit eddie = StdLit.fromUnescaped("Eddie", xsdString);
        StdLit stanford = StdLit.fromUnescaped("Stanford", xsdString);
        StdLit mit = StdLit.fromUnescaped("MIT", xsdString);
        StdLit i22 = StdLit.fromUnescaped("22", xsdInteger);
        StdLit i25 = StdLit.fromUnescaped("25", xsdInteger);
        StdLit i26 = StdLit.fromUnescaped("26", xsdInteger);
        return Stream.of(
                // test URI assignment from id
                asList("data-1.csv", createQuery(x, nameEx, alice),
                       singleton(MapSolution.build(x, new StdURI(EX+"inst/1")))),
                // test URI assingment and obj-var bind
                asList("data-1.csv", createQuery(x, nameEx, alice, x, ageEx, y),
                        singleton(MapSolution.builder()
                                .put(x, new StdURI(EX+"inst/1"))
                                .put(y, i22).build())),
                // test projection
                asList("data-1.csv",
                       createQuery(x, nameEx, alice, x, ageEx, y, Projection.of("y")),
                       singleton(MapSolution.build(y, i22))),
                // test object-object match
                asList("data-1.csv",
                       createQuery(x, nameEx, alice,
                                   x, university, u,
                                   y, nameEx, v,
                                   y, university, u, Projection.of("v")),
                        newHashSet(MapSolution.build(v, bob),
                                   MapSolution.build(v, alice))),
                // test distinct
                asList("data-1.csv",
                       createQuery(x, university, u, Projection.of("u"), Distinct.INSTANCE),
                       newHashSet(MapSolution.build(u, stanford), MapSolution.build(u, mit))),
                // test filter
                asList("data-1.csv",
                       createQuery(x, nameEx, v, x, ageEx, y, SPARQLFilter.build("?y > 24")),
                       newHashSet(
                               MapSolution.builder()
                                       .put(x, new StdURI(EX+"inst/4"))
                                       .put(v, dave)
                                       .put(y, i25).build(),
                               MapSolution.builder()
                                       .put(x, new StdURI(EX+"inst/5"))
                                       .put(v, eddie)
                                       .put(y, i26).build())),
                // test object-object join with FILTER
                asList("data-1.csv",
                        createQuery(x, nameEx, charlie,
                                    x, university, u,
                                    y, nameEx, v,
                                    y, ageEx, z,
                                    y, university, u,
                                    Projection.of("v"), SPARQLFilter.build("?z > 24")),
                        newHashSet(MapSolution.build(v, dave),
                                   MapSolution.build(v, eddie))),
                // test object-object join with FILTER and distinct
                asList("data-1.csv",
                        createQuery(x, nameEx, charlie,
                                    x, university, u,
                                    y, nameEx, v,
                                    y, ageEx, z,
                                    y, university, u,
                                    Projection.of("u"), SPARQLFilter.build("?z > 24"),
                                    Distinct.INSTANCE),
                        singleton(MapSolution.build(u, mit)))
                ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull String csvPath, @Nonnull CQuery query,
                          @Nonnull Collection<Solution> expected) throws IOException {
        ContextMapping.TableBuilder builder = ContextMapping.builder().beginTable("T")
                .instancePrefix(EX + "inst/")
                .fallbackPrefix(EX);
        if (hasId(csvPath))
            builder.addIdColumn("id");
        ContextMapping mapping = builder.endTable().build();
        CSVInMemoryCQEndpoint ep;
        try (InputStream in = getClass().getResourceAsStream(csvPath)) {
            assertNotNull(in);
            ep = CSVInMemoryCQEndpoint.loader(mapping).load(in, StandardCharsets.UTF_8);
        }

        Set<Solution> actual = new HashSet<>();
        ep.query(query).forEachRemainingThenClose(actual::add);
        assertEquals(actual, new HashSet<>(expected));
    }

    private boolean hasId(@Nonnull String csvPath) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
        try (InputStream in = getClass().getResourceAsStream(csvPath);
             Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
             CSVParser parser = new CSVParser(reader, format)) {
            return parser.getHeaderNames().contains("id");
        }
    }
}