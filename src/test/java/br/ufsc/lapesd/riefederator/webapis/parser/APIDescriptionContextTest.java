package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.MappedJsonResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.OnlyNumbersTermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimitsRegistry;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.util.Collections;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class APIDescriptionContextTest {

    @Test
    public void testEmpty() {
        APIDescriptionContext context = new APIDescriptionContext();
        assertNull(context.getResponseParser("/list", MediaType.APPLICATION_JSON_TYPE));
        assertNull(context.getPagingStrategy("/list"));
        assertNull(context.getRateLimitRegistry("/test"));
        assertNull(context.getSerializer("/list", "category"));
    }

    @Test
    public void testGetInputSerializer() {
        APIDescriptionContext context = new APIDescriptionContext();
        OnlyNumbersTermSerializer s4 = OnlyNumbersTermSerializer.builder().setWidth(4).build();
        OnlyNumbersTermSerializer s5 = OnlyNumbersTermSerializer.builder().setWidth(5).build();
        context.setSerializer(".*/list", "group", s5);
        context.setSerializer(".*/list", "age", s4);
        context.setSerializer("users/list", "group", s4);

        assertNull(context.getSerializer("/test", "x"));
        assertNull(context.getSerializer("/list", "x"));
        assertSame(context.getSerializer("/list", "group"), s5);
        assertSame(context.getSerializer("/list", "age"), s4);
        assertSame(context.getSerializer("users/list", "group"), s4);

        context.unsetSerializer(".*/list", "age");
        assertSame(context.getSerializer("/list", "group"), s5);
        assertNull(context.getSerializer("/list", "age"));
        assertSame(context.getSerializer("users/list", "group"), s4);
    }

    @Test
    public void testRateLimit() {
        APIDescriptionContext context = new APIDescriptionContext();
        RateLimitsRegistry r1 = new RateLimitsRegistry();
        RateLimitsRegistry r2 = new RateLimitsRegistry();
        context.setRateLimitRegistry(r1);
        context.setRateLimitRegistry("/test/?.*", r2);

        assertSame(context.getRateLimitRegistry("/test"), r2);
        assertSame(context.getRateLimitRegistry("/test/2"), r2);
        assertSame(context.getRateLimitRegistry("/"), r1);

        context.unsetRateLimitRegistry("/test/?.*");
        assertSame(context.getRateLimitRegistry("/test"), r1);
        assertSame(context.getRateLimitRegistry("/test/2"), r1);
        assertSame(context.getRateLimitRegistry("/"), r1);

        context.unsetRateLimitRegistry();
        assertNull(context.getRateLimitRegistry("/test"));
        assertNull(context.getRateLimitRegistry("/test/2"));
        assertNull(context.getRateLimitRegistry("/"));
    }

    @Test
    public void testResponseParser() {
        APIDescriptionContext context = new APIDescriptionContext();
        MappedJsonResponseParser p1, p2;
        p1 = new MappedJsonResponseParser(Collections.emptyMap(), "urn:plain");
        p2 = new MappedJsonResponseParser(Collections.emptyMap(), "urn:plain");
        context.setResponseParser(MediaType.APPLICATION_JSON_TYPE, p1);
        context.setResponseParser("/list", MediaType.APPLICATION_JSON_TYPE, p2);

        assertSame(context.getResponseParser("/other", "application/json"), p1);
        assertSame(context.getResponseParser("/list", "application/json"), p2);

        context.unsetResponseParser("/list", MediaType.APPLICATION_JSON_TYPE);

        assertSame(context.getResponseParser("/other", "application/json"), p1);
        assertSame(context.getResponseParser("/list", "application/json"), p1);
    }
}