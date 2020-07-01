package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.parser.SwaggerParser;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.lang.Double.parseDouble;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ArrayParametersServiceTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    private static final String RESOURCES_BASE = "br/ufsc/lapesd/riefederator/webapis";
    private static final String swaggerYaml = RESOURCES_BASE + "/array-parameters-service.yaml";

    private static final StdPlain value = new StdPlain("value");

    @Path("/")
    public static class Service {
        private static int parseBound(@Nonnull List<String> list, int index, int fallback) {
            if (list.size() <= index) return fallback;
            String string = list.get(index).trim();
            return string.isEmpty() ? fallback : Integer.parseInt(string);
        }

        @GET @Path("integers") @Produces(APPLICATION_JSON)
        public @Nonnull String testGetIntegers(@QueryParam("range") String range) {
            List<String> list = Splitter.on('|').splitToList(range);
            int min = parseBound(list, 0, 0 );
            int max = parseBound(list, 1, 10);
            StringBuilder builder = new StringBuilder("{\"value\": [");
            for (int i = min; i < max; i++)
                builder.append(i).append(", ");
            if (min != max)
                builder.setLength(builder.length()-2);
            builder.append("]}");
            return builder.toString();
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(Service.class);
    }

    @Test
    public void selfTestGetRange() throws IOException {
        String json = target("/integers").queryParam("range", "2|5")
                .request(APPLICATION_JSON).get(String.class);
        List<?> list = DictTree.load().fromJsonString(json).getListNN("value");
        assertEquals(list, asList(2.0, 3.0, 4.0));
    }

    @Test
    public void selfTestGetOpenRight() throws IOException {
        String json = target("/integers").queryParam("range", "2|")
                .request(APPLICATION_JSON).get(String.class);
        List<?> list = DictTree.load().fromJsonString(json).getListNN("value");
        assertEquals(list, asList(2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
    }

    @Test
    public void selfTestGetOpenLeft() throws IOException {
        String json = target("/integers").queryParam("range", "|3")
                .request(APPLICATION_JSON).get(String.class);
        List<?> list = DictTree.load().fromJsonString(json).getListNN("value");
        assertEquals(list, asList(0.0, 1.0, 2.0));
    }

    @Test
    public void testParseSwagger() throws IOException {
        SwaggerParser parser = SwaggerParser.getFactory().fromResource(swaggerYaml);
        WebAPICQEndpoint ep = parser.getEndpoint("/integers");
        assertNotNull(ep);
    }

    private @Nonnull SwaggerParser createParser() throws IOException {
        SwaggerParser parser = SwaggerParser.getFactory().fromResource(swaggerYaml);
        URI rootUri = target().getUri();
        parser.setHost(rootUri.getHost()+":"+rootUri.getPort());
        return parser;
    }

    @Test
    public void testQueryRange() throws IOException {
        SwaggerParser parser = createParser();
        WebAPICQEndpoint ep = parser.getEndpoint("/integers");
        Set<Integer> ac = new HashSet<>();
        ep.query(createQuery(x, value, y,
                             SPARQLFilter.build("?y >= 2"), SPARQLFilter.build("?y <= 5"))
        ).forEachRemainingThenClose(s ->
                ac.add((int) parseDouble(requireNonNull(s.get(y)).asLiteral().getLexicalForm())));
        assertEquals(ac, Sets.newHashSet(2, 3, 4));
    }

    @Test
    public void testQueryOpenRight() throws IOException {
        SwaggerParser parser = createParser();
        WebAPICQEndpoint ep = parser.getEndpoint("/integers");
        Set<Integer> ac = new HashSet<>();
        ep.query(createQuery(x, value, y,
                SPARQLFilter.build("?y >= 7"))
        ).forEachRemainingThenClose(s ->
                ac.add((int) parseDouble(requireNonNull(s.get(y)).asLiteral().getLexicalForm())));
        assertEquals(ac, Sets.newHashSet(7, 8, 9));
    }

    @Test
    public void testQueryOpenLeft() throws IOException {
        SwaggerParser parser = createParser();
        WebAPICQEndpoint ep = parser.getEndpoint("/integers");
        Set<Integer> ac = new HashSet<>();
        ep.query(createQuery(x, value, y,
                SPARQLFilter.build("?y <= 3"))
        ).forEachRemainingThenClose(s ->
                ac.add((int) parseDouble(requireNonNull(s.get(y)).asLiteral().getLexicalForm())));
        assertEquals(ac, Sets.newHashSet(0, 1, 2));
    }

}
