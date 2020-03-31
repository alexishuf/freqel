package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.util.ResourceOpener;
import br.ufsc.lapesd.riefederator.webapis.parser.SwaggerParser;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

@Path("/")
public class ProcurementsService {
    private static final Logger logger = LoggerFactory.getLogger(ProcurementsService.class);

    private static final @Nonnull List<String> IDS = Arrays.asList(
            "267291791",
            "270989389",
            "271598497",
            "277815533",
            "278614622"
    );

    private static final  @Nonnull ThreadLocal<SimpleDateFormat> DATE_FMT
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("dd/MM/yyyy"));

    public static @Nonnull SwaggerParser getSwaggerParser(@Nonnull WebTarget rootUri) throws IOException {
        String resourcePath = "portal_transparencia-ext.yaml";
        DictTree tree = DictTree.load().fromResource(ProcurementsService.class, resourcePath);

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

    public static @Nonnull WebAPICQEndpoint
    getProcurementsClient(@Nonnull WebTarget root) throws IOException {
        return getSwaggerParser(root).getEndpoint("/api-de-dados/licitacoes");
    }


    public static @Nonnull WebAPICQEndpoint
    getProcurementsOptClient(@Nonnull WebTarget root) throws IOException {
        return getSwaggerParser(root).getEndpoint("/api-de-dados/licitacoes-opt");
    }

    public static @Nonnull WebAPICQEndpoint
    getProcurementsByIdClient(@Nonnull WebTarget root) throws IOException {
        return getSwaggerParser(root).getEndpoint("/api-de-dados/licitacoes/{id}");
    }

    private static @Nullable Date parseDate(@Nullable String string) throws ParseException {
        if (string == null) return null;
        try {
            return DATE_FMT.get().parse(string);
        } catch (ParseException e) {
            logger.info("Badly formatted date: {}", string, e);
            throw e;
        }
    }

    private static boolean compare(@Nonnull DictTree tree, @Nonnull String path, Object bound,
                                   @Nonnull Predicate<Integer> predicate) {
        Object value = tree.getPrimitive(path, null);
        if (value == null || bound == null)
            return (value == null) == (bound == null);
        if (bound instanceof Number) {
            if (!(value instanceof Number))
                value = Double.parseDouble(value.toString());
            return predicate.test(Double.compare(((Number)value).doubleValue(),
                    ((Number)bound).doubleValue()));
        } else if (bound instanceof Date) {
            try {
                Date dateValue = DATE_FMT.get().parse(value.toString());
                return predicate.test(dateValue.compareTo((Date) bound));
            } catch (ParseException e) {
                logger.warn("Expected a date, but actual value {} failed to parse as " +
                            "date with format {}. Comparison will fail.", value, DATE_FMT.get());
                return false;
            }
        } else {
            return predicate.test(value.toString().compareTo(bound.toString()));
        }
    }

    private static boolean isFilteredOut(@Nonnull DictTree dict,
                                         @Nonnull Map<String, Object> filter,
                                         @Nonnull Predicate<Integer> predicate) {
        for (Map.Entry<String, Object> e : filter.entrySet()) {
            if (!compare(dict, e.getKey(), e.getValue(), predicate))
                return true;
        }
        return false;
    }


    private @Nonnull Response getProcurements(@Nonnull Map<String, Object> filter,
                                              @Nonnull Map<String, Object> min,
                                              @Nonnull Map<String, Object> max) throws IOException {
        List<Map<String, Object>> selected = new ArrayList<>();
        for (String id : IDS) {
            String path = "procurements/" + id + ".json";
            DictTree tree;
            tree = DictTree.load().fromResource(getClass(), path);

            if (isFilteredOut(tree, filter, i -> i == 0)) continue;
            if (isFilteredOut(tree, min,    i -> i >= 0)) continue;
            if (isFilteredOut(tree, max,    i -> i <= 0)) continue;
            selected.add(tree.asMap());
        }
        return wrapResponse(selected);
    }

    private @Nonnull Response wrapResponse(@Nonnull List<Map<String, Object>> selected) {
        String json = new GsonBuilder().registerTypeAdapter(Double.class,
                (JsonSerializer<Double>) (src, typeOfSrc, c)
                        -> new JsonPrimitive(BigDecimal.valueOf(src))
        ).setPrettyPrinting().create().toJson(selected);
        return Response.ok(json, APPLICATION_JSON).build();
    }

    @GET
    @Path("/api-de-dados/licitacoes")
    public @Nonnull Response
    getProcurements(@QueryParam("dataInicial") String startDateString,
                    @QueryParam("dataFinal") String endDateString,
                    @QueryParam("codigoOrgao") String code,
                    @QueryParam("pagina") int page) throws IOException {
        if (page > 1)
            return wrapResponse(emptyList());
        Map<String, Object> filter = new HashMap<>();
        Map<String, Object> min = new HashMap<>(), max = new HashMap<>();
        filter.put("unidadeGestora/orgaoVinculado/codigoSIAFI", code);

        Date startDate, endDate;
        try {
            startDate = parseDate(startDateString);
            endDate = parseDate(endDateString);
        } catch (ParseException e) {
            String reason = "Bad date, should be dd/MM/yyyy";
            return Response.noContent().status(500, reason).build();
        }
        if (startDate  != null)
            min.put("dataAbertura", startDate);
        if (endDate != null)
            max.put("dataAbertura", endDate);
        return getProcurements(filter, min, max);
    }

    @GET
    @Path("/api-de-dados/licitacoes-opt")
    public @Nonnull Response
    getProcurementsOpt(@QueryParam("minValor") double minValor,
                       @QueryParam("maxValor") double maxValor,
                       @QueryParam("dataInicial") String startDateString,
                       @QueryParam("dataFinal") String endDateString,
                       @QueryParam("codigoOrgao") String code,
                       @QueryParam("codigoSituacao") int situationCode,
                       @QueryParam("pagina") int page) throws IOException {
        if (page > 1)
            return wrapResponse(emptyList());
        Map<String, Object> filter = new HashMap<>(), min = new HashMap<>(), max = new HashMap<>();
        if (code != null)
            filter.put("unidadeGestora/orgaoVinculado/codigoSIAFI", code);
        if (situationCode != 0)
            filter.put("situacaoCompra/codigo", situationCode);

        Date startDate, endDate;
        try {
            startDate = parseDate(startDateString);
            endDate = parseDate(endDateString);
        } catch (ParseException e) {
            String reason = "Bad date, must be dd/MM/yyyy";
            return Response.noContent().status(500, reason).build();
        }
        if (startDate != null) min.put("dataAbertura", startDate);
        if (  endDate != null) max.put("dataAbertura",   endDate);
        if (minValor != 0) min.put("valor", minValor);
        if (maxValor != 0) max.put("valor", maxValor);

        return getProcurements(filter, min, max);
    }

    @GET
    @Path("api-de-dados/licitacoes/{id}")
    public @Nonnull Response getProcurementById(@PathParam("id") String id) {
        String path = "procurements/" + id + ".json";
        try (InputStream stream = ResourceOpener.getStream(getClass(), path)) {
            String json = IOUtils.toString(stream, StandardCharsets.UTF_8);
            return Response.ok(json, APPLICATION_JSON_TYPE).build();
        } catch (IOException e) {
            return Response.noContent().status(Response.Status.NOT_FOUND).build();
        }
    }
}
