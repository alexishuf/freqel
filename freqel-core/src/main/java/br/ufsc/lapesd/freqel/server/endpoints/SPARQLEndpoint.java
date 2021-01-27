package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.parse.UnsupportedSPARQLFeatureException;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.server.sparql.ResultsFormatterDispatcher;
import org.apache.commons.io.output.StringBuilderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.PrintWriter;

import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

@Path("/sparql")
public class SPARQLEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLEndpoint.class);

    public static final @Nonnull MediaType JSON_TYPE =
            new MediaType("application", "sparql-results+json");

    private @Context Application application;

    private @Nonnull Federation getFederation() {
        String key = Federation.class.getName();
        Object obj = application.getProperties().get(key);
        if (obj == null)
            throw new IllegalArgumentException("Property "+ key +" not set");
        if (!(obj instanceof Federation))
            throw new IllegalArgumentException("Property "+ key +" is not a Federation");
        return (Federation)obj;
    }

    private @Nonnull Response handleQuery(@Nullable String query,
                                          @Nullable HttpHeaders headers, UriInfo uriInfo) {
        query = query == null ? "" : query;
        try {
            Op parsed = SPARQLParser.tolerant().parse(query);
            Results results = getFederation().query(parsed);
            return ResultsFormatterDispatcher.getDefault()
                    .format(results, parsed.modifiers().ask() != null, headers, uriInfo)
                    .toResponse().build();
        } catch (UnsupportedSPARQLFeatureException e) {
            return createExceptionResponse(query, "Unsupported SPARQL Feature", e);
        } catch (SPARQLParseException e) {
            return createExceptionResponse(query, "Query Syntax Error", e);
        } catch (Throwable t) { //includes QueryExecutionException
            return createExceptionResponse(query, "Query Execution Failed", t);
        }
    }

    private @Nonnull Response createExceptionResponse(@Nonnull String query,
                                                      @Nonnull String reason,
                                                      @Nonnull Throwable t) {
        StringBuilderWriter stringBuilderWriter = new StringBuilderWriter();
        t.printStackTrace(new PrintWriter(stringBuilderWriter));
        String trace = stringBuilderWriter.toString();
        String message = format("Execution of query failed: %s\n" +
                "Query:\n" +
                "%s\n" +
                "Traceback:\n" +
                "%s\n", t.getMessage(), query, trace);
        return Response.status(500, reason).type(TEXT_PLAIN_TYPE).entity(message).build();
    }

    @GET
    @Path("query")
    public @Nonnull Response queryGet(@QueryParam("query") String query, @Context UriInfo uriInfo,
                                      @Context HttpHeaders headers) {
        try {
            return handleQuery(query, headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing GET {}", uriInfo.getRequestUri(), e);
            throw e;
        }
    }

    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Path("query")
    public @Nonnull Response queryForm(@FormParam("query") String query, @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        try {
            return handleQuery(query, headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing POST " +
                        "application/x-www-form-urlencoded {}", uriInfo.getRequestUri(), e);
            throw e;
        }
    }

    @POST
    @Consumes("application/sparql-query")
    @Path("query")
    public @Nonnull Response queryPost(String query, @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        try {
            return handleQuery(query, headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing POST application/sparql-query {}",
                    uriInfo.getRequestUri(), e);
            throw e;

        }
    }
}
