package br.ufsc.lapesd.riefederator.webapis.requests.impl.paging;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.NoPagingStrategy;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.Collections;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.testng.Assert.*;

public class NoPagingStrategyTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {

    @Path("/")
    public static class Service {
        @GET
        @Path("square/{x}")
        @Produces("application/json")
        public @Nonnull String square(@PathParam("x") int x) {
            return String.format("{\"result\": %d}", x*x);
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(Service.class);
    }

    @Test
    public void selfTest() {
        String jsonString = target("/square/3").request(APPLICATION_JSON).get(String.class);
        JsonElement element = new JsonParser().parse(jsonString);
        assertTrue(element.isJsonObject());
        assertEquals(element.getAsJsonObject().get("result").getAsInt(), 9);
    }

    @Test
    public void testNoParam() {
        assertEquals(NoPagingStrategy.INSTANCE.getParametersUsed(), Collections.emptyList());
    }

    @Test
    public void testAtEndIdempotent() {
        PagingStrategy.Pager pager = new NoPagingStrategy().createPager();
        assertFalse(pager.atEnd());
        Solution original = MapSolution.build("x", new StdURI("http://example.org/Alice"));
        Solution copy = pager.apply(original);
        assertEquals(copy, original);
        assertFalse(pager.atEnd());
        assertFalse(pager.atEnd());
    }

    @Test
    public void testNotifyResponseAdvances() {
        PagingStrategy.Pager pager = new NoPagingStrategy().createPager();
        assertFalse(pager.atEnd());

        Solution original = MapSolution.build("x", Alice);
        Solution copy = pager.apply(original);
        assertEquals(copy, MapSolution.build("x", Alice));
        assertFalse(pager.atEnd());

        Response response = target("/square/3").request(APPLICATION_JSON).get();
        pager.notifyResponse(response);
        assertTrue(pager.atEnd());
    }

    @Test
    public void testNotifyEndpointAdvances() {
        PagingStrategy.Pager pager = new NoPagingStrategy().createPager();
        assertFalse(pager.atEnd());

        Solution original = MapSolution.build("x", new StdURI("http://example.org/Alice"));
        Solution copy = pager.apply(original);
        assertEquals(copy, original);
        assertFalse(pager.atEnd());

        pager.notifyResponseEndpoint(new EmptyEndpoint());
        assertTrue(pager.atEnd());
    }


    @Test
    public void testBothNotifiesAdvance() {
        PagingStrategy.Pager pager = new NoPagingStrategy().createPager();
        assertFalse(pager.atEnd());
        Response response = target("/square/2").request(APPLICATION_JSON).get();
        pager.notifyResponse(response);
        assertTrue(pager.atEnd());
        pager.notifyResponseEndpoint(new EmptyEndpoint());
        assertTrue(pager.atEnd());
    }
}