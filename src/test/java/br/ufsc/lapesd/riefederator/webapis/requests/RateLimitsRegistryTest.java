package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimit;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimitsRegistry;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.impl.NoRateLimit;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.impl.SimpleRateLimit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertSame;

public class RateLimitsRegistryTest {
    private RateLimitsRegistry registry;
    private RateLimit a;
    private RateLimit b;
    private RateLimit secure;
    private RateLimit secureOld;
    private RateLimit pathCD;
    private RateLimit auth;
    private RateLimit port;
    private RateLimit global;
    private RateLimit globalOld;
    private RateLimit portOld;

    @BeforeClass
    public void setUp() {
        a = SimpleRateLimit.perMinute(10);
        b = SimpleRateLimit.perMinute(20);
        secure    = SimpleRateLimit.perMinute(30);
        secureOld = SimpleRateLimit.perMinute(31);
        port    = SimpleRateLimit.perMinute(40);
        portOld = SimpleRateLimit.perMinute(41);
        global    = SimpleRateLimit.perMinute(50);
        globalOld = SimpleRateLimit.perMinute(51);
        pathCD = SimpleRateLimit.perMinute(60);
        auth    = SimpleRateLimit.perMinute(70);
        RateLimit pathCDOld = SimpleRateLimit.perMinute(61);
        RateLimit authOld = SimpleRateLimit.perMinute(71);

        registry = new RateLimitsRegistry();
        registry.register("a.example.org", a);
        registry.register("b.example.org", b);
        registry.register("http://plain.example.org/", secureOld);
        registry.register("https://secure.example.org/", secure);
        registry.register("https://path.example.org/a/b", pathCDOld);
        registry.register("https://path.example.org/c/d", pathCD);
        registry.register("https://auth.example.org/", authOld);
        registry.register("https://user:pass@auth.example.org/", auth);
        registry.register("https://port.example.org/", portOld);
        registry.register("https://port.example.org:8080/", port);
        registry.register("global.example.org", globalOld);
        RateLimitsRegistry.getGlobal().register("global.example.org", global);
    }

    @AfterClass
    public void tearDown() {
        RateLimitsRegistry.getGlobal().register("global.example.org", null);
    }

    @Test
    public void testAddAndRemove() {
        RateLimitsRegistry r = new RateLimitsRegistry();
        String A_HOST = "a.example.org", B_HOST = "b.example.org";

        assertSame(r.register(A_HOST, a), null);
        assertSame(r.get(A_HOST), a);
        assertSame(r.get("https://"+A_HOST+"/path?x=1"), a);

        assertSame(r.register(A_HOST, b), a);
        assertSame(r.get(A_HOST), b);
        assertSame(r.get("https://"+A_HOST+"/path?x=1"), b);

        assertSame(r.register(A_HOST, a), b);
        assertSame(r.get(A_HOST), a);
        assertSame(r.get("https://"+A_HOST+"/path?x=1"), a);

        assertSame(r.register(B_HOST, b), null);
        assertSame(r.get(B_HOST), b);

        assertSame(r.unregister(B_HOST), b);
        assertSame(r.get(B_HOST), NoRateLimit.INSTANCE);
        assertSame(r.get("https://"+B_HOST), NoRateLimit.INSTANCE);
        assertSame(r.get("https://"+B_HOST+"/path"), NoRateLimit.INSTANCE);

        assertSame(r.unregister(B_HOST), null);

        assertSame(r.register("https://"+B_HOST, b), null);
        assertSame(r.get(B_HOST), b);
        assertSame(r.get("https://"+B_HOST+"/asd?x=1"), b);

        assertSame(r.get(A_HOST), a);
        assertSame(r.get("https://"+A_HOST+"/asd?x=1"), a);
    }

    @Test
    public void testFallbackToGlobal() {
        String uri = "https://global.example.org/asd#22?x=1";
        assertSame(registry.get(uri), globalOld);
        registry.unregister(uri);
        assertSame(registry.get(uri), global);
    }

    @Test
    public void testGetUnregistered() {
        assertSame(registry.get("https://unregistered.example.org/"), NoRateLimit.INSTANCE);
        assertSame(registry.get("http://unregistered.example.org/"), NoRateLimit.INSTANCE);
        assertSame(registry.get("https://user:pwd@unregistered.example.org/"), NoRateLimit.INSTANCE);
        assertSame(registry.get("https://unregistered.example.org/asd"), NoRateLimit.INSTANCE);
        assertSame(registry.get("https://unregistered.example.org/asd?x=1"), NoRateLimit.INSTANCE);
    }

    @Test
    public void testGetSubDomains() {
        assertSame(registry.get("https://a.example.org/path"), a);
        assertSame(registry.get("https://b.example.org/path"), b);
        assertSame(registry.get("https://a.example.org/path"), a);
        assertSame(registry.get("https://b.example.org/path"), b);
    }

    @Test
    public void testGetHostname() {
        assertSame(registry.get("a.example.org"), a);
        assertSame(registry.get("b.example.org"), b);
    }

    @Test
    public void testIgnorePaths() {
        assertSame(registry.get("https://path.example.org/a/b"), pathCD);
        assertSame(registry.get("https://path.example.org/a/x"), pathCD);
        assertSame(registry.get("https://path.example.org/a/x?y=1"), pathCD);
    }

    @Test
    public void testDoesNotIgnorePort() {
        assertSame(registry.get("https://port.example.org/"), portOld);
        assertSame(registry.get("https://port.example.org:8080/"), port);
        assertSame(registry.get("https://port.example.org:8090/"), NoRateLimit.INSTANCE);
        assertSame(registry.get("http://port.example.org/asd/?x=1#f"), portOld);
        assertSame(registry.get("http://port.example.org:8080/asd/?x=1#f"), port);
    }

    @Test
    public void testIgnoreAuth() {
        assertSame(registry.get("https://auth.example.org/"), auth);
        assertSame(registry.get("https://bob:pwd@auth.example.org/"), auth);
        assertSame(registry.get("https://bob:pwd@auth.example.org/asd"), auth);
    }

    @Test
    public void testIgnoreHttps() {
        assertSame(registry.get("http://plain.example.org/"), secureOld);
        assertSame(registry.get("https://plain.example.org/"), secureOld);
        assertSame(registry.get("https://plain.example.org/asd"), secureOld);
        assertSame(registry.get("https://secure.example.org/"), secure);
        assertSame(registry.get("http://secure.example.org/"), secure);
        assertSame(registry.get("http://secure.example.org/asd"), secure);
    }
}