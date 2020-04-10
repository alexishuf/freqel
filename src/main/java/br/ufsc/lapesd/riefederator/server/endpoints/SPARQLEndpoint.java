package br.ufsc.lapesd.riefederator.server.endpoints;

import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.QueryExecutionException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser;
import br.ufsc.lapesd.riefederator.query.parse.UnsupportedSPARQLFeatureException;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.server.sparql.ResultsFormatterDispatcher;
import org.apache.commons.io.output.StringBuilderWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.PrintWriter;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

@Path("/sparql")
public class SPARQLEndpoint {
    public static final @Nonnull MediaType JSON_TYPE =
            new MediaType("application", "sparql-results+json");
    private static final @Nonnull List<Variant> ALLOWED_VARIANTS =
            singletonList(new Variant(JSON_TYPE, (String) null, null));

    private @Context Application application;

    private @Nonnull Federation getFederation() {
        String key = Federation.class.getName();
        Object obj = application.getProperties().get(key);
        checkArgument(obj != null, "Property "+ key +" not set");
        checkArgument(obj instanceof Federation, "Property "+ key +" is not a Federation");
        return (Federation)obj;
    }

    private @Nonnull Response handleQuery(@Nullable String query,
                                          @Nullable HttpHeaders headers, UriInfo uriInfo) {
        List<MediaType> acceptable;
        acceptable = headers == null ? emptyList() : headers.getAcceptableMediaTypes();
        if (!acceptable.isEmpty() && acceptable.stream().noneMatch(JSON_TYPE::isCompatible))
            return Response.notAcceptable(ALLOWED_VARIANTS).build();

        query = query == null ? "" : query;
        try {
            CQuery cQuery = SPARQLQueryParser.parse(query);
            Results results = getFederation().query(cQuery);
            return ResultsFormatterDispatcher.getDefault()
                    .format(results, cQuery.isAsk(), headers, uriInfo)
                    .toResponse().build();
        } catch (QueryExecutionException e) {
            StringBuilderWriter stringBuilderWriter = new StringBuilderWriter();
            e.printStackTrace(new PrintWriter(stringBuilderWriter));
            String trace = stringBuilderWriter.toString();
            String message = format("Execution of query failed: %s\n" +
                    "Query:\n" +
                    "%s\n" +
                    "Traceback:\n" +
                    "%s\n", e.getMessage(), query, trace);
            return Response.status(500, "Query execution failed")
                    .type(TEXT_PLAIN_TYPE).entity(message).build();
        } catch (UnsupportedSPARQLFeatureException e) {
            return Response.status(500, "Unsupported SPARQL feature")
                    .type(TEXT_PLAIN_TYPE)
                    .entity(e.getMessage() + "\nQuery:\n" + query).build();
        } catch (SPARQLParseException e) {
            return Response.status(500, "Bad query syntax")
                    .type(TEXT_PLAIN_TYPE)
                    .entity(e.getMessage() + "\n Query:\n" + query).build();
        }
    }

    @GET
    @Path("query")
    public @Nonnull Response queryGet(@QueryParam("query") String query, @Context UriInfo uriInfo,
                                      @Context HttpHeaders headers) {
        return handleQuery(query, headers, uriInfo);
    }

    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Path("query")
    public @Nonnull Response queryForm(@FormParam("query") String query, @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        return handleQuery(query, headers, uriInfo);
    }

    @POST
    @Consumes("application/sparql-query")
    @Path("query")
    public @Nonnull Response queryPost(String query, @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        return handleQuery(query, headers, uriInfo);
    }
}
