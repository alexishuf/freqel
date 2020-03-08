package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.util.ResourceOpener;
import br.ufsc.lapesd.riefederator.webapis.parser.SwaggerParser;
import com.google.common.base.Preconditions;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    public static @Nonnull SwaggerParser getSwaggerParser(@Nonnull WebTarget rootUri) throws IOException {
        String resourcePath = "portal_transparencia-ext.yaml";
        DictTree tree = DictTree.load().fromResource(ProcurementsService.class, resourcePath);

        tree.put("host", rootUri.getUri().getHost());
        String path = rootUri.getUri().getPath();
        Preconditions.checkArgument(path.isEmpty() || path.equals("/"),
                                    "Path must be empty or be /");
        tree.put("basePath", path);
        tree.remove("schemes");
        return SwaggerParser.FACTORY.fromDict(tree);
    }

    public static @Nonnull
    WebAPICQEndpoint getProcurementsClient(@Nonnull WebTarget root) throws IOException {
        return getSwaggerParser(root).getEndpoint("/api-de-dados/licitacoes");
    }

    public static @Nonnull
    WebAPICQEndpoint getProcurementsByIdClient(@Nonnull WebTarget root) throws IOException {
        return getSwaggerParser(root).getEndpoint("/api-de-dados/licitacoes/{id}");
    }



    @GET
    @Path("/api-de-dados/licitacoes")
    public @Nonnull Response
    getProcurements(@QueryParam("dataInicial") String startDateString,
                    @QueryParam("dataFinal") String endDateString,
                    @QueryParam("codigoOrgao") String code) throws IOException {
        String codePath = "unidadeGestora/orgaoVinculado/codigoSIAFI";
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        Date startDate = null, endDate;
        try {
            startDate = dateFormat.parse(startDateString);
            endDate = dateFormat.parse(endDateString);
        } catch (ParseException e) {
            logger.info("Badly formatted date: {}",
                         startDate == null ? startDateString : endDateString, e);
            return Response.noContent().status(500, "Bad date, should be dd/MM/yyyy").build();
        }
        List<Map<String, Object>> selected = new ArrayList<>();
        for (String id : IDS) {
            String path = "procurements/" + id + ".json";
            DictTree tree;
            tree = DictTree.load().fromResource(getClass(), path);
            if (!tree.getPrimitive(codePath, "").equals(code))
                continue;
            String openDateString = tree.getPrimitive("dataAbertura", "").toString();
            try {
                Date openDate = dateFormat.parse(openDateString);
                if (openDate.before(startDate) || openDate.after(endDate))
                    continue;
            } catch (ParseException e) {
                logger.error("Ignoring badly formatted dataAbertura={} for procurement {}",
                             openDateString, id, e);
                continue;
            }
            selected.add(tree.asMap());
        }
        String json = new GsonBuilder().setPrettyPrinting().create().toJson(selected);
        return Response.ok(json, APPLICATION_JSON).build();
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
