package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.jena.model.vocab.SPARQLSD;
import br.ufsc.lapesd.freqel.jena.rs.ModelMessageBodyWriter;
import br.ufsc.lapesd.freqel.query.modifiers.Reasoning;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.parse.UnsupportedSPARQLFeatureException;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.reason.regimes.EntailmentRegime;
import br.ufsc.lapesd.freqel.reason.regimes.SourcedEntailmentRegime;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import br.ufsc.lapesd.freqel.server.sparql.ResultsFormatterDispatcher;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.ADVERTISED_REASONING;
import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.CROSS_SOURCE;
import static br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes.SIMPLE;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

@Path("/sparql")
public class SPARQLEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(SPARQLEndpoint.class);
    private static final @Nonnull List<MediaType> PREFERRED_RDF_TYPES;

    static {
        ArrayList<String> list = new ArrayList<>(ModelMessageBodyWriter.getSupportedContentTypes());
        for (String ct : asList("application/ld+json", "application/n-quads", "application/trig",
                "application/n-triples", "text/turtle")) {
            list.remove(ct);
            list.add(0, ct);
        }
        PREFERRED_RDF_TYPES = list.stream().map(MediaType::valueOf).collect(Collectors.toList());
    }

    public static final @Nonnull MediaType JSON_TYPE =
            new MediaType("application", "sparql-results+json");

    private @Context Application application;
    private SourcedEntailmentRegime reasoningRegime;
    private String reasoningGraphIRI;

    private @Nonnull Federation getFederation() {
        String key = Federation.class.getName();
        Object obj = application.getProperties().get(key);
        if (obj == null)
            throw new IllegalArgumentException("Property "+ key +" not set");
        if (!(obj instanceof Federation))
            throw new IllegalArgumentException("Property "+ key +" is not a Federation");
        return (Federation)obj;
    }

    private @Nonnull SourcedEntailmentRegime getReasoningRegime() {
        if (reasoningRegime == null) {
            reasoningRegime = getFederation().getFreqelConfig()
                    .get(ADVERTISED_REASONING, SourcedEntailmentRegime.class);
        }
        return reasoningRegime;
    }

    private @Nonnull String getReasoningGraphIRI() {
        if (reasoningGraphIRI != null)
            return reasoningGraphIRI;
        SourcedEntailmentRegime sr = getReasoningRegime();
        EntailmentRegime regime = sr.regime();
        if (regime instanceof W3CEntailmentRegimes.W3CRegime)
            return reasoningGraphIRI = ((W3CEntailmentRegimes.W3CRegime)regime).getGraphIRI();
        Escaper escaper = UrlEscapers.urlPathSegmentEscaper();
        return reasoningGraphIRI = V.Freqel.Entailment.Graph.NS + escaper.escape(regime.name());
    }

    private @Nonnull Response handleQuery(@Nullable String query, boolean reason,
                                          @Nullable HttpHeaders headers, UriInfo uriInfo) {
        query = query == null ? "" : query;
        try {
            Op parsed = SPARQLParser.tolerant().parse(query);
            if (reason)
                parsed.modifiers().add(Reasoning.INSTANCE);
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
    public @Nonnull Response get(@QueryParam("query") String query, @Context UriInfo uriInfo,
                                 @Context HttpHeaders headers) {
        return queryGet(query, uriInfo, headers);
    }

    @POST @Consumes("application/x-www-form-urlencoded")
    public @Nonnull Response form(@FormParam("query") String query,
                                  @FormParam("default-graph-iri") List<String> defGraphIRIs,
                                  @FormParam("named-graph-iri") List<String> namedGraphIRIs,
                                  @Context UriInfo uriInfo,
                                  @Context HttpHeaders headers) {
        return queryForm(query, defGraphIRIs, namedGraphIRIs, uriInfo, headers);
    }

    @POST @Consumes("application/sparql-query")
    public @Nonnull Response post(String query, @Context UriInfo uriInfo,
                                  @Context HttpHeaders headers) {
        return queryPost(query, uriInfo, headers);
    }

    @GET @Path("query")
    public @Nonnull Response queryGet(@QueryParam("query") String query, @Context UriInfo uriInfo,
                                      @Context HttpHeaders headers) {
        try {
            if (query == null) {
                return serviceDescription(headers, uriInfo);
            }
            return handleQuery(query, reasoningRequested(uriInfo), headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing GET {}", uriInfo.getRequestUri(), e);
            throw e;
        }
    }

    private boolean reasoningRequested(@Nonnull UriInfo uriInfo) {
        MultivaluedMap<String, String> ps = uriInfo.getQueryParameters();
        String rIRI = getReasoningGraphIRI();
        List<String> el = Collections.emptyList();
        return ps.getOrDefault("default-graph-uri", el).contains(rIRI)
                || ps.getOrDefault("named-graph-uri", el).contains(rIRI);
    }

    private @Nonnull Resource createReasoningGraph(@Nonnull Model model, @Nonnull String baseURI,
                                                   @Nonnull SourcedEntailmentRegime sr) {
        Escaper escaper = UrlEscapers.urlPathSegmentEscaper();
        String graphName = V.Freqel.Entailment.Graph.NS + escaper.escape(sr.regime().name());
        Resource namedGraph = model
                .createResource(baseURI + "/entailed/" + sr.regime().name())
                .addProperty(RDF.type, SPARQLSD.NamedGraph);
        namedGraph.addProperty(SPARQLSD.name, namedGraph)
                .addProperty(SPARQLSD.name, model.createResource(graphName))
                .addProperty(SPARQLSD.entailmentRegime, model.createResource(sr.regime().iri()))
                .addProperty(model.createProperty(V.Freqel.Entailment.evidences.getURI()),
                             sr.evidences().name());
        if (sr.regime().profileIRI() != null)
            namedGraph.addProperty(SPARQLSD.supportedEntailmentProfile, sr.regime().profileIRI());
        return namedGraph;
    }

    private @Nonnull Response serviceDescription(@Nonnull HttpHeaders headers,
                                                 @Nonnull UriInfo uriInfo) {
        MediaType rdfMediaType = chooseRDFMediaType(headers);
        if (rdfMediaType == null) {
            return Response.status(Response.Status.NOT_ACCEPTABLE)
                    .type(TEXT_PLAIN_TYPE)
                    .entity("No RDF representation supported by this server matches the given " +
                            "Accept header value "+
                            join(", ", headers.getRequestHeader("Accept")))
                    .build();
        }

        Model model = ModelFactory.createDefaultModel();
        String uri = uriInfo.getRequestUri().toString().replaceAll("/$", "");
        Federation federation = getFederation();
        SourcedEntailmentRegime simpleRegime = new SourcedEntailmentRegime(CROSS_SOURCE, SIMPLE);
        Resource simpleGraph = createReasoningGraph(model, uri, simpleRegime);
        Resource dataset = model.createResource(uri + "/dataset")
                .addProperty(RDF.type, SPARQLSD.Dataset)
                .addProperty(SPARQLSD.defaultGraph,
                        model.createResource().addProperty(RDF.type, SPARQLSD.Graph))
                .addProperty(SPARQLSD.namedGraph, simpleGraph);
        SourcedEntailmentRegime sr = federation.getFreqelConfig()
                .get(ADVERTISED_REASONING, SourcedEntailmentRegime.class);
        if (sr != null && !sr.regime().equals(SIMPLE))
            dataset.addProperty(SPARQLSD.namedGraph, createReasoningGraph(model, uri, sr));

        model.createResource(uri)
                .addProperty(SPARQLSD.endpoint, model.createResource(uri))
                .addProperty(RDF.type, SPARQLSD.Service)
                .addProperty(SPARQLSD.resultFormat, SPARQLSD.JSON_RESULTS)
                .addProperty(SPARQLSD.resultFormat, SPARQLSD.XML_RESULTS)
                .addProperty(SPARQLSD.resultFormat, SPARQLSD.TSV_RESULTS)
                .addProperty(SPARQLSD.resultFormat, SPARQLSD.CSV_RESULTS)
                .addProperty(SPARQLSD.defaultEntailmentRegime, model.createResource(SIMPLE.iri()))
                .addProperty(SPARQLSD.defaultDataset, dataset);
        return Response.ok(model, rdfMediaType).build();
    }

    /**
     * Choose best match for RDF representation content type among the "Accept" header values.
     *
     * @param headers request headers
     * @return A MediaType handled by {@link ModelMessageBodyWriter} or null of there is no
     *         acceptable content type.
     */
    private @Nullable MediaType chooseRDFMediaType(@Nonnull HttpHeaders headers) {
        for (MediaType r : headers.getAcceptableMediaTypes()) {
            for (MediaType o : PREFERRED_RDF_TYPES) {
                if (r.isCompatible(o)) {
                    if (r.isWildcardType() || r.isWildcardSubtype())
                        return new MediaType(o.getType(), o.getSubtype(), r.getParameters());
                    else
                        return r;
                }
            }
        }
        return null;
    }

    @POST
    @Path("query")
    @Consumes("application/x-www-form-urlencoded")
    public @Nonnull Response queryForm(@FormParam("query") String query,
                                       @FormParam("default-graph-iri") List<String> defGraphIRIs,
                                       @FormParam("named-graph-iri") List<String> namedGraphIRIs,
                                       @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        try {
            String rIRI = getReasoningGraphIRI();
            boolean reason = defGraphIRIs.contains(rIRI) || namedGraphIRIs.contains(rIRI);
            return handleQuery(query, reason, headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing POST " +
                    "application/x-www-form-urlencoded {}", uriInfo.getRequestUri(), e);
            throw e;
        }
    }

    @POST
    @Path("query")
    @Consumes("application/sparql-query")
    public @Nonnull Response queryPost(String query, @Context UriInfo uriInfo,
                                       @Context HttpHeaders headers) {
        try {
            return handleQuery(query, reasoningRequested(uriInfo), headers, uriInfo);
        } catch (Exception e) {
            logger.warn("Exception thrown while processing POST application/sparql-query {}",
                    uriInfo.getRequestUri(), e);
            throw e;

        }
    }
}
