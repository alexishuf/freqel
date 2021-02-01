package br.ufsc.lapesd.freqel.cnc;

import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.util.ResourceOpener;
import br.ufsc.lapesd.freqel.webapis.parser.SwaggerParser;
import com.google.common.base.Stopwatch;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

@Path("/")
public class CncService {
    private static final Logger logger = LoggerFactory.getLogger(CncService.class);

    private static final String DEFAULT_DT_RESOURCE = "../cnc-dtwin.json";
    private static final String DEFAULT_CSV_RESOURCE = "../experiment_01.csv";
    private static final String DEFAULT_SEPARATOR = ",";
    private static final String DEFAULT_PRODUCT_MODEL = "C1000";
    private static final String DEFAULT_SERIAL_NUMBER = "X78Y32989934";

    private Map<Long, Sample> sampleMap;
    private String digitalTwinJson = null;

    public Map<Long,Sample> getSampleMap() {
        return this.sampleMap;
    }

    public List<Sample> getSampleList() {
        return new ArrayList<>(this.sampleMap.values());
    }

    public String getDigitalTwinJson() {
        return this.digitalTwinJson;
    }

    public CncService() {
        this(DEFAULT_DT_RESOURCE, DEFAULT_CSV_RESOURCE, DEFAULT_SEPARATOR);
    }

    public CncService(String dtPath, String csvPath, String separator) {
        try (InputStream stream = ResourceOpener.getStream(getClass(), dtPath)) {
            this.digitalTwinJson = IOUtils.toString(stream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Error reading file {}.", dtPath);
        }

        try (InputStream stream = ResourceOpener.getStream(getClass(), csvPath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, UTF_8));
            SampleLoader sampleLoader = new SampleLoader();
            this.sampleMap = sampleLoader.readSampleMapFromCSV(reader, separator);
            for(Sample sample: this.sampleMap.values()){
                sample.setProductModel(DEFAULT_PRODUCT_MODEL);
                sample.setSerialNumber(DEFAULT_SERIAL_NUMBER);
            }
        } catch (IOException e) {
            logger.error("Error reading file {}.", csvPath);
            this.sampleMap = new HashMap<>();
        }
    }

    public static @Nonnull SwaggerParser getSwaggerParser(@Nonnull WebTarget rootUri) throws IOException {
        String resourcePath = "cnc.yaml";
        DictTree tree = DictTree.load().fromResource(CncService.class, resourcePath);

        String host = rootUri.getUri().getHost();
        if (rootUri.getUri().getPort() > 0)
            host += ":" + rootUri.getUri().getPort();
        tree.getMapNN("overlay").put("host", host);
        String path = rootUri.getUri().getPath();
        checkArgument(path.isEmpty() || path.equals("/"),
                "Path must be empty or be /");
        tree.getMapNN("overlay").put("basePath", path);
        tree.remove("schemes");
        SwaggerParser swaggerParser = SwaggerParser.FACTORY.fromDict(tree);
        assert Objects.equals(swaggerParser.getHost(), host);
        return swaggerParser;
    }

    private @Nonnull Response wrapResponse(@Nonnull List<Map<String,Object>> selected) {

        String json = new GsonBuilder().registerTypeAdapter(Double.class,
                (JsonSerializer<Double>) (src, typeOfSrc, c)
                        -> new JsonPrimitive(BigDecimal.valueOf(src))
        ).setPrettyPrinting().create().toJson(selected);
        return Response.ok(json, APPLICATION_JSON).build();
    }

    private @Nonnull Response wrapResponse(@Nonnull Map<String,Object> selected) {
        String json = new GsonBuilder().registerTypeAdapter(Double.class,
                (JsonSerializer<Double>) (src, typeOfSrc, c)
                        -> new JsonPrimitive(BigDecimal.valueOf(src))
        ).setPrettyPrinting().create().toJson(selected);
        return Response.ok(json, APPLICATION_JSON).build();
    }

    @GET
    @Path("/cnc-api/samples/")
    public @Nonnull Response getSamples(@Context UriInfo uriInfo) {
        logger.info("Request received: " + uriInfo.getRequestUri());
        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();

        Stopwatch sw = Stopwatch.createStarted();
        try {
            List<Map<String,Object>> workingSet =
                    SampleMatcher.filterSamples(getSampleList(), queryParams, uriInfo);
            return wrapResponse(workingSet);
        } catch (NumberFormatException ex) {
            String reason = "Bad request format: parameter values could not be parsed.";
            return Response.noContent().status(500, reason).build();
        } finally {
            logger.info("Request on /cnc-api/samples took {}ms",
                    sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        }
    }

    @GET
    @Path("/cnc-api/samples/{id}")
    public @Nonnull Response
    getSample(@PathParam("id") long id, @Context UriInfo uriInfo) {
        logger.info("Request received: " + uriInfo.getRequestUri());
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Sample sample = this.getSampleMap().get(id);
            if (sample != null)
                return wrapResponse(sample.toMap(uriInfo));
            return Response.noContent().status(Response.Status.NOT_FOUND).build();
        } finally {
            logger.info("Request on /cnc-api/samples/{} took {}ms", id,
                    sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        }
    }

    @GET
    @Path("/cnc-api/dtwin")
    public @Nonnull Response getDigitalTwin() {
        if (this.digitalTwinJson != null)
            return Response.ok(this.digitalTwinJson, APPLICATION_JSON_TYPE).build();
        return Response.noContent().status(Response.Status.NOT_FOUND).build();
    }
}
