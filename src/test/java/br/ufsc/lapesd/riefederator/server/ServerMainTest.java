package br.ufsc.lapesd.riefederator.server;

import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.server.utils.PercentEncoder;
import br.ufsc.lapesd.riefederator.util.ChildJVM;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.TransparencyService;
import br.ufsc.lapesd.riefederator.webapis.TransparencyServiceTestContext;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;
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
    private File tempDir;
    private @Nullable ChildJVM server;
    private final String RES_ROOT = "br/ufsc/lapesd/riefederator/";

    @Override
    protected Application configure() {
        return new ResourceConfig().register(TransparencyService.class);
    }

    private @Nonnull File extractResource(@Nonnull String resourcePath) throws IOException {
        File file = new File(tempDir, resourcePath.replaceAll("^.*/([^/]+)$", "$1"));
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (FileOutputStream out = new FileOutputStream(file);
             InputStream in = cl.getResourceAsStream(resourcePath)) {
            assertNotNull(in, "resource "+resourcePath+" not found");
            IOUtils.copy(in, out);
        }
        return file;
    }

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        URI uri = target().getUri();
        String testHost = uri.getHost() + ":" + uri.getPort();
        tempDir = Files.createTempDirectory("riefederator").toFile();
        String webapis = RES_ROOT + "webapis/";
        extractResource(webapis + "portal_transparencia.json");

        File yaml = extractResource(webapis + "portal_transparencia-ext.yaml");
        String yamlString = IOUtils.toString(new FileInputStream(yaml), UTF_8);
        yamlString = yamlString.replaceAll("overlay: *", "overlay:\n  host: \""+testHost+"\"");
        try (FileOutputStream out = new FileOutputStream(yaml);
             OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8)) {
            writer.write(yamlString);
        }

        extractResource(RES_ROOT + "modalidades.ttl");
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir);
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
            Thread.sleep(200);
        } catch (InterruptedException ignored) { }
        return port;
    }

    private String waitForListening(ChildJVM process) throws Exception {
        Pattern rx = Pattern.compile("SPARQL endpoint listening on (http://[^/]+/sparql/query)");
        Stopwatch sw = Stopwatch.createStarted();
        BufferedReader reader = process.getStdOutReader();
        while (sw.elapsed(SECONDS) < 30) {
            String line = reader.readLine();
            Matcher matcher = rx.matcher(line);
            if (matcher.find())
                return matcher.group(1);
        }
        process.close(); //timeout!
        String msg = "waitForListening timed out after " + sw.elapsed(SECONDS) + " seconds";
        throw new AssertionError(msg);
    }

    @Test
    public void testProcurementOfContractsNoServer() throws Exception {
        File config = extractResource(RES_ROOT + "server/budget-scenario-test.yaml");
        Federation federation = new FederationSpecLoader().load(config);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String sparqlResourcePath = RES_ROOT + "federation/transparency-query-2.sparql";
        String sparql;
        try (InputStream in = cl.getResourceAsStream(sparqlResourcePath)) {
            assertNotNull(in, "Resource "+sparqlResourcePath+" not found!");
            sparql = IOUtils.toString(in, UTF_8);
        }
        CQuery query = SPARQLQueryParser.parse(sparql);
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
        File config = extractResource(RES_ROOT + "server/budget-scenario-test.yaml");
        int port = getAvailablePort();
        server = ChildJVM.start(ServerMain.class,
                "--address", "localhost",
                "--port", String.valueOf(port),
                "--config", config.getAbsolutePath());
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