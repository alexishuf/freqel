package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.MappedJsonResponseParser;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class MappedJsonResponseParserTest implements TestContext {
    private static final @Nonnull String EX = "http://example.org/";
    private static final @Nonnull String AEX = "http://auto.example.org/";
    private static final @Nonnull URI pa = ex("p/a");
    private static final @Nonnull URI pb = ex("p/b");
    private static final @Nonnull Lit i23 = fromJena(createTypedLiteral(23));
    private static final @Nonnull Lit i27 = fromJena(createTypedLiteral(27));
    private static final @Nonnull Lit i31 = fromJena(createTypedLiteral(31));

    private MappedJsonResponseParser parser;
    private MappedJsonResponseParser parserWithPrefix;

    private static @Nonnull StdURI ex(@Nonnull String local) {
        return new StdURI(EX+local);
    }

    @BeforeMethod
    public void setUp() {
        Map<String, String> map = new HashMap<>();
        map.put("prop_a", EX + "p/a");
        map.put("prop_b", EX + "p/b");
        parser = new MappedJsonResponseParser(map);
        parserWithPrefix = new MappedJsonResponseParser(map, AEX);
    }

    @AfterMethod
    public void tearDown() {
        parser = null;
    }

    @DataProvider
    public  static Object[][] emptyData() {
        return new Object[][] {
                new Object[] {""},
                new Object[] {"{}"},
                new Object[] {"{\"p_a\": 1, \"p_b\": 2}"},
                new Object[] {"[]"},
                new Object[] {"[{\"p_a\": 1, \"p_b\": 2}]"},
        };
    }

    @Test(dataProvider = "emptyData")
    public void testEmptyString(String json) {
        CQEndpoint ep = parser.parse(json, EX + "res/1");
        assertNull(ep); //parser realizes it generated and empty graph
    }

    @Test
    public void testPlainWithExtras() {
        CQEndpoint ep = parser.parse("{\"prop_a\": 1, \"prop_c\": 2}", EX + "res/1");
        assertNotNull(ep);
        Triple triple = new Triple(x, y, z);
        Set<Solution> all = new HashSet<>();
        try (Results results = ep.query(CQuery.from(triple))) {
            results.forEachRemaining(all::add);
        }

        Set<MapSolution> expected = singleton(MapSolution.builder()
                .put(x, ex("res/1"))
                .put(y, pa)
                .put(z, fromJena(createTypedLiteral(1))).build());
        assertEquals(all, expected);
    }

    @Test
    public void testBlankNode() {
        CQEndpoint ep = parser.parse("{\"prop_a\": {\"prop_b\": 23}}", EX + "res/1");
        assertNotNull(ep);
        try (Results r1 = ep.query(CQuery.from(new Triple(ex("res/1"), pa, x)))) {
            assertTrue(r1.hasNext());
            Solution next = r1.next();
            Term x = next.get(MappedJsonResponseParserTest.x.getName());
            assertNotNull(x);
            assertTrue(x.isBlank());
        }

        try (Results r2 = ep.query(CQuery.from(new Triple(x, pb, i23)))) {
            assertTrue(r2.hasNext());
            Term x = r2.next().get(MappedJsonResponseParserTest.x.getName());
            assertNotNull(x);
            assertTrue(x.isBlank());
        }

        Set<Solution> all = new HashSet<>();
        try (Results r3 = ep.query(new Triple(x, y, z))) {
            r3.forEachRemaining(all::add);
        }
        assertEquals(all.size(), 2);
    }

    @Test
    public void testHAL() {
        CQEndpoint ep = parser.parse("{\n" +
                "  \"prop_a\": {\n" +
                "    \"_links\": {\n" +
                "      \"self\": {\n" +
                "        \"href\": \"http://example.org/res/23\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"prop_b\": 23\n" +
                "  }\n" +
                "}\n", EX + "res/2");
        assertNotNull(ep);
        Set<Solution> all = new HashSet<>();
        try (Results results = ep.query(CQuery.from(new Triple(x, y, z)))) {
            results.forEachRemaining(all::add);
        }

        StdURI r23 = ex("res/23");
        HashSet<MapSolution> expected = Sets.newHashSet(
                MapSolution.builder().put(x, ex("res/2")).put(y, pa).put(z, r23).build(),
                MapSolution.builder().put(x, r23).put(y, pb).put(z, i23).build());
        assertEquals(all, expected);
    }

    @Test
    public void testRelSelfLinks() {
        for (List<String> scenario : asList(asList("", "" ), asList("_", "" ),
                                            asList("", "["), asList("_", "["))) {
            String u = scenario.get(0), ob = scenario.get(1);
            String cb = ob.equals("[") ? "]" : "";
            CQEndpoint ep = parser.parse("{\n" +
                    "  \"prop_a\": {\n" +
                    "    \""+u+"links\": "+ob+" {\n" +
                    "      \""+u+"rel\": \"self\",\n" +
                    "      \""+u+"href\": \"http://example.org/res/23\"\n" +
                    "    } "+cb+",\n" +
                    "    \"prop_b\": 23\n" +
                    "  }\n" +
                    "}\n", EX + "res/2");
            assertNotNull(ep);
            Set<Solution> all = new HashSet<>();
            try (Results results = ep.query(CQuery.from(new Triple(x, y, z)))) {
                results.forEachRemaining(all::add);
            }

            StdURI r23 = ex("res/23");
            HashSet<MapSolution> expected = Sets.newHashSet(
                    MapSolution.builder().put(x, ex("res/2")).put(y, pa).put(z, r23).build(),
                    MapSolution.builder().put(x, r23).put(y, pb).put(z, i23).build());
            assertEquals(all, expected);
        }
    }

    @Test
    public void testHALOverrides() {
        CQEndpoint ep = parser.parse("{\n" +
                "  \"prop_a\": 23,\n" +
                "  \"_links\": {\n" +
                "    \"self\": {\n" +
                "      \"href\": \"http://example.org/res/37\"\n" +
                "    }\n" +
                "  }\n" +
                "}\n", EX + "res/1");
        assertNotNull(ep);
        Set<Solution> all = new HashSet<>();
        try (Results results = ep.query(CQuery.from(new Triple(x, y, z)))) {
            results.forEachRemaining(all::add);
        }

        assertEquals(all, singleton(
                MapSolution.builder().put(x, ex("res/37")).put(y, pa).put(z, i23).build()));
    }

    @Test
    public void testFallbackToPrefixOnEmbedded() {
        CQEndpoint ep = parserWithPrefix.parse("{\n" +
                "  \"prop_a\": 23,\n" +
                "  \"_embedded\": {\n" +
                "    \"prop_b\": {\n" +
                "      \"_links\": {\n" +
                "        \"self\": {\n" +
                "          \"href\": \"http://example.org/res/5\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"prop_a\": 27,\n" +
                "      \"prop_c\": 31\n" +
                "    }\n" +
                "  }\n" +
                "}\n", EX + "res/1");
        assertNotNull(ep);
        Set<Solution> all = new HashSet<>();
        try (Results results = ep.query(CQuery.from(new Triple(x, y, z)))) {
            results.forEachRemaining(all::add);
        }

        StdURI pc = new StdURI(AEX + "prop_c");
        HashSet<Solution> expected = Sets.newHashSet(
                MapSolution.builder().put(x, ex("res/1")).put(y, pa).put(z, i23).build(),
                MapSolution.builder().put(x, ex("res/1")).put(y, pb).put(z, ex("res/5")).build(),
                MapSolution.builder().put(x, ex("res/5")).put(y, pa).put(z, i27).build(),
                MapSolution.builder().put(x, ex("res/5")).put(y, pc).put(z, i31).build()
        );
        assertEquals(all, expected);
    }
}