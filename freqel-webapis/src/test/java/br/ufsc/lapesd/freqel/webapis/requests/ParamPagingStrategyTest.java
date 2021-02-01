package br.ufsc.lapesd.freqel.webapis.requests;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.freqel.webapis.requests.paging.impl.ParamPagingStrategy;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.MappedJsonResponseParser;
import org.apache.jena.rdf.model.ModelFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.*;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.*;

public class ParamPagingStrategyTest
        extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    private static final String EXAMPLE_NS = "http://example.org/";
    private static final StdLit i1 = StdLit.fromUnescaped("1", xsdInt);
    private static final StdLit i2 = StdLit.fromUnescaped("2", xsdInt);
    private static final StdLit i3 = StdLit.fromUnescaped("3", xsdInt);

    @Path("/")
    public static class Service {
        @GET
        @Path("count/{pages}")
        @Produces("application/json")
        public @Nonnull String square(@PathParam("pages") int pages, @QueryParam("page") int page) {
            if (page <= pages) {
                return String.format("{\"value\": %d}", page);
            } else if (page == pages+1) {
                return "{}";
            } else if (page == pages+2) {
                return "";
            } else {
                fail("Unexpected service call with page="+page);
                return "";
            }
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(Service.class);
    }

    @Test
    public void selfTest() {
        List<String> jsonList = new ArrayList<>(), expected = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            jsonList.add(target("count/3").queryParam("page", i)
                    .request(APPLICATION_JSON).get(String.class));
            expected.add(String.format("{\"value\": %d}", i));
        }
        expected.set(3, "{}");
        expected.set(4, "");
        assertEquals(jsonList, expected);

        Response response = target("count/3").queryParam("page", 5)
                .request(APPLICATION_JSON).get();
        assertEquals(response.getLength(), 0);
    }

    @Test
    public void testDefaultEndEarlyOnFirst() {
        List<CQEndpoint> list = Arrays.asList(
                null,
                new EmptyEndpoint(),
                ARQEndpoint.forModel(ModelFactory.createDefaultModel()));
        for (CQEndpoint ep : list) {
            ParamPagingStrategy strategy = ParamPagingStrategy.builder().withParam("page").build();
            PagingStrategy.Pager pager = strategy.createPager();

            assertFalse(pager.atEnd());
            Solution solution = pager.apply(MapSolution.build("pages", i2));
            assertEquals(solution, MapSolution.builder().put("pages", i2).put("page", i1).build());

            pager.notifyResponseEndpoint(ep);
            assertTrue(pager.atEnd(), String.format("ep=%s", ep));
        }
    }

    @Test
    public void testDefaultConsumeAll() {
        List<Boolean> endOnEmptyJsonList = Arrays.asList(false, true);

        for (Boolean endOnEmptyJson : endOnEmptyJsonList) {
            MappedJsonResponseParser responseParser;
            responseParser = new MappedJsonResponseParser(Collections.emptyMap(), EXAMPLE_NS);
            ParamPagingStrategy strategy = ParamPagingStrategy.builder().withParam("page")
                    .withEndOnEmptyJson(endOnEmptyJson).build();
            assertEquals(strategy.getParametersUsed(), Collections.singletonList("page"));

            PagingStrategy.Pager pager = strategy.createPager();

            for (int i = 1; i <= 4; i++) {
                assertFalse(pager.atEnd());

                Solution bindings = pager.apply(MapSolution.build("pages", i3));
                StdLit iLit = StdLit.fromUnescaped(String.valueOf(i), xsdInt);
                assertEquals(bindings,
                        MapSolution.builder().put("pages", i3).put("page", iLit).build());

                WebTarget target = target("count/3").queryParam("page", i);
                String hint = target.getUri().toString();
                Response response = target.request(APPLICATION_JSON).get();

                pager.notifyResponse(response);

                Object object = response.readEntity(responseParser.getDesiredClass());
                CQEndpoint endpoint = responseParser.parse(object, hint);

                pager.notifyResponseEndpoint(endpoint);
            }
            assertTrue(pager.atEnd());
        }
    }
}