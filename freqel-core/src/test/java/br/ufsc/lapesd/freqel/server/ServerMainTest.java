package br.ufsc.lapesd.freqel.server;

import br.ufsc.lapesd.freqel.TempDir;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.server.utils.PercentEncoder;
import br.ufsc.lapesd.freqel.util.ChildJVM;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.webapis.TransparencyService;
import br.ufsc.lapesd.freqel.webapis.TransparencyServiceTestContext;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.ServerSocket;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.InetAddress.getLocalHost;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.*;

/**
 * Integration test for the SPARQL endpoint
 */
public class ServerMainTest extends JerseyTestNg.ContainerPerClassTest
        implements TransparencyServiceTestContext {
    private @Nullable ChildJVM server;
    private TempDir tempDir;
    private final String RES_ROOT = "br/ufsc/lapesd/freqel/";

    @Override
    protected Application configure() {
        return new ResourceConfig().register(TransparencyService.class);
    }

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        URI uri = target().getUri();
        String testHost = uri.getHost() + ":" + uri.getPort();
        String webapis = RES_ROOT + "webapis/";
        tempDir = new TempDir();
        tempDir.extractResource(webapis + "portal_transparencia.json");

        File yaml = tempDir.extractResource(webapis + "portal_transparencia-ext.yaml");
        String yamlString = IOUtils.toString(new FileInputStream(yaml), UTF_8);
        yamlString = yamlString.replaceAll("overlay: *", "overlay:\n  host: \""+testHost+"\"");
        try (FileOutputStream out = new FileOutputStream(yaml);
             OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8)) {
            writer.write(yamlString);
        }

        tempDir.extractResource(RES_ROOT + "modalidades.ttl");
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (tempDir != null) {
            tempDir.close();
            tempDir = null;
        }
        if (server != null) {
            server.close();
            server = null;
        }
    }

    private int getAvailablePort() {
        int port = 4041;
        try (ServerSocket serverSocket = new ServerSocket(0, 50, getLocalHost())) {
            port = serverSocket.getLocalPort();
        } catch (IOException ignored) { }
        try {
            Thread.sleep(300);
        } catch (InterruptedException ignored) { }
        return port;
    }

    private String waitForListening(ChildJVM process) throws Exception {
        Pattern rx = Pattern.compile("SPARQL endpoint listening on (http://[^/]+/sparql/query)");

        BufferedReader reader = process.getStdOutReader();
        CompletableFuture<String> uriFuture = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                String line = reader.readLine();
                for (; line != null; line = reader.readLine()) {
                    Matcher matcher = rx.matcher(line);
                    if (matcher.find()) {
                        uriFuture.complete(matcher.group(1));
                        break;
                    }
                }
            } catch (IOException e) {
                uriFuture.completeExceptionally(e);
            }
        });
        thread.start();
        try {
            return uriFuture.get(30, SECONDS);
        } catch (ExecutionException|TimeoutException e) {
            try {
                thread.interrupt();
                process.close();
                thread.join(10000);
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    @Test
    public void testProcurementOfContractsNoServer() throws Exception {
        File config = tempDir.extractResource(RES_ROOT + "server/budget-scenario-test.yaml");
        Federation federation = new FederationSpecLoader().load(config);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String sparqlResourcePath = RES_ROOT + "federation/transparency-query-2.sparql";
        String sparql;
        try (InputStream in = cl.getResourceAsStream(sparqlResourcePath)) {
            assertNotNull(in, "Resource "+sparqlResourcePath+" not found!");
            sparql = IOUtils.toString(in, UTF_8);
        }
        Op query = SPARQLParser.strict().parse(sparql);
        Set<Map<String, String>> solutions = new HashSet<>();
        Results results = federation.query(query);
        while (results.hasNext()) {
            Solution solution = results.next();
            Map<String, String> map = new HashMap<>();
            solution.forEach((n, t) -> map.put(n, t.asLiteral().getLexicalForm()));
            solutions.add(map);
        }

        Set<Map<String, String>> expectedSolutions = new HashSet<>();
        Map<String, String> map = new HashMap<>();
        map.put("id", "70507179");
        map.put("startDate", "2019-12-02");
        map.put("openDate", "2019-10-17");
        map.put("modDescr", "Pregão - Registro de Preço");
        expectedSolutions.add(new HashMap<>(map));
        map.put("id", "71407155");
        expectedSolutions.add(new HashMap<>(map));
        assertEquals(solutions, expectedSolutions);
    }

    @Test
    public void testProcurementOfContracts() throws Exception {
        File config = tempDir.extractResource(RES_ROOT + "server/budget-scenario-test.yaml");
        int port = getAvailablePort();
        server = ChildJVM.builder(ServerMain.class)
                .addArguments("--address", "localhost")
                .addArguments("--port", String.valueOf(port))
                .addArguments("--config", config.getAbsolutePath())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        String sparqlEp = waitForListening(server);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String sparqlResourcePath = RES_ROOT + "federation/transparency-query-2.sparql";
        String sparql;
        try (InputStream in = cl.getResourceAsStream(sparqlResourcePath)) {
            assertNotNull(in, "Resource "+sparqlResourcePath+" not found!");
            sparql = PercentEncoder.encode(IOUtils.toString(in, UTF_8));
        }
        String json;
        try {
            json = ClientBuilder.newClient().target(sparqlEp)
                    .queryParam("query", sparql)
                    .request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        } catch (WebApplicationException e) {
            server.close();
            throw e;
        }
        DictTree tree = DictTree.load().fromJsonString(json);
        HashSet<String> vars = Sets.newHashSet("id", "startDate", "openDate", "modDescr");
        assertEquals(tree.getSetNN("head/vars"), vars);

        Set<Map<String, String>> solutions = new HashSet<>();
        List<Object> bindings = tree.getListNN("results/bindings");
        for (Object bindingObj : bindings) {
            assertTrue(bindingObj instanceof DictTree);
            DictTree binding = (DictTree) bindingObj;
            Map<String, String> solution = new HashMap<>();
            for (String var : vars)
                solution.put(var, binding.getString(var+"/value"));
            solutions.add(solution);
        }
        Set<Map<String, String>> expectedSolutions = new HashSet<>();
        Map<String, String> map = new HashMap<>();
        map.put("id", "70507179");
        map.put("startDate", "2019-12-02");
        map.put("openDate", "2019-10-17");
        map.put("modDescr", "Pregão - Registro de Preço");
        expectedSolutions.add(map);
        map.put("id", "71407155");
        expectedSolutions.add(map);
        assertEquals(solutions, expectedSolutions);
    }
}