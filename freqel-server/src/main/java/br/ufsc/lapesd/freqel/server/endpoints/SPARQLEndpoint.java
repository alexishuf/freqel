package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.jena.model.vocab.SPARQLSD;
import br.ufsc.lapesd.freqel.jena.rs.ModelMessageBodyWriter;
import br.ufsc.lapesd.freqel.query.modifiers.Reasoning;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.reason.regimes.EntailmentRegime;
import br.ufsc.lapesd.freqel.reason.regimes.SourcedEntailmentRegime;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import br.ufsc.lapesd.freqel.server.results.ChunkedEncoder;
import br.ufsc.lapesd.freqel.server.results.ChunkedEncoderRegistry;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.net.MediaType;
import com.google.common.net.UrlEscapers;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.vocabulary.RDF;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.ADVERTISED_REASONING;
import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.CROSS_SOURCE;
import static br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes.SIMPLE;
import static com.google.common.net.MediaType.parse;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;


@SuppressWarnings("UnstableApiUsage")
public class SPARQLEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(SPARQLEndpoint.class);
    private static final @Nonnull Pattern OUT_PARAM_RX = Pattern.compile("[?&](?:format|out(?:put)?)=([^&#]*)");
    private static final @Nonnull List<MediaType> PREFERRED_RDF_TYPES;
    private static final @Nonnull Comparator<MediaType> REV_Q_COMPARATOR;
    private static final @Nonnull List<MediaType> PREFERRED_RESULT_TYPES = asList(
            parse("text/tab-separated-values"),
            parse("application/sparql-results+json"),
            parse("text/csv"));
    private static final String QUERY_TYPE = "application/sparql-query";
    private static final String FORM_TYPE = "application/x-www-form-urlencoded";

    static {
        ArrayList<String> list = new ArrayList<>(ModelMessageBodyWriter.getSupportedContentTypes());
        for (String ct : asList("application/ld+json", "application/n-quads", "application/trig",
                "application/n-triples", "text/turtle")) {
            list.remove(ct);
            list.add(0, ct);
        }
        PREFERRED_RDF_TYPES = list.stream().map(MediaType::parse).collect(Collectors.toList());

        Comparator<MediaType> comparator = Comparator.comparing(m -> {
            ImmutableList<String> qs = m.parameters().get("q");
            try {
                return qs.isEmpty() ? 1.0 : Double.parseDouble(qs.get(0));
            } catch (NumberFormatException e) {
                throw new RequestException(406, "Bad q-value for" + m);
            }
        });
        REV_Q_COMPARATOR = comparator.reversed();
    }

    private final @Nonnull Federation federation;
    private final String reasoningGraphIRI;
    private final @Nonnull ChunkedEncoderRegistry encoderRegistry;
    private final SPARQLParser parser = SPARQLParser.tolerant();
    private final @Nonnull List<MediaType> resultTypes;

    public SPARQLEndpoint(@Nonnull Federation federation) {
        this(federation, ChunkedEncoderRegistry.get());
    }

    public SPARQLEndpoint(@Nonnull Federation federation,
                          @Nonnull ChunkedEncoderRegistry encoderRegistry) {
        this.federation = federation;
        this.encoderRegistry = encoderRegistry;
        SourcedEntailmentRegime reasoningRegime = federation.getFreqelConfig()
                .get(ADVERTISED_REASONING, SourcedEntailmentRegime.class);
        this.reasoningGraphIRI = getReasoningGraphIRI(reasoningRegime);
        List<MediaType> list = encoderRegistry.supportedTypes().stream()
                                              .map(MediaType::parse).collect(Collectors.toList());
        ListIterator<MediaType> it;
        it = PREFERRED_RESULT_TYPES.listIterator(PREFERRED_RESULT_TYPES.size());
        while (it.hasPrevious()) {
            MediaType mt = it.previous();
            list.remove(mt);
            list.add(0, mt);
        }
        this.resultTypes = list;
    }

    private static @Nonnull String getReasoningGraphIRI(@Nonnull SourcedEntailmentRegime sr) {
        EntailmentRegime regime = sr.regime();
        if (regime instanceof W3CEntailmentRegimes.W3CRegime)
            return ((W3CEntailmentRegimes.W3CRegime)regime).getGraphIRI();
        Escaper escaper = UrlEscapers.urlPathSegmentEscaper();
        return V.Freqel.Entailment.Graph.NS + escaper.escape(regime.name());
    }

    private static @Nonnull String unescape(@Nonnull String str) {
        return unescape(str, 0, str.length());
    }

    private static @Nonnull String unescape(@Nonnull String str, int begin, int end) {
        StringBuilder b = new StringBuilder(str.length()*2);
        if (!str.contains("%20"))
            str = str.replace('+', ' ');
        while (begin < end) {
            int i = str.indexOf('%', begin);
            int pctIndex = i < 0 ? end : i;
            b.append(str, begin, pctIndex);
            if (pctIndex+2 < end) {
                try {
                    char ch = (char) parseInt(str.substring(pctIndex+1, pctIndex+3), 16);
                    b.append(ch);
                } catch (NumberFormatException e) {
                    String msg = "Bad escape: \""+str.substring(pctIndex, pctIndex+3)+"\"";
                    throw new RequestException(400, msg);
                }
                begin = pctIndex+3;
            } else if (pctIndex < end) {
                String bad = str.substring(pctIndex, end);
                String msg = "Invalid %-escape \""+bad+"\": unexpected end-of-string";
                throw new RequestException(400, msg);
            } else {
                begin = pctIndex;
            }
        }
        return b.toString();
    }

    private @Nonnull Mono<ByteBuf> createError(@Nonnull HttpServerResponse response,
                                               @Nonnull HttpResponseStatus status,
                                               @Nullable Throwable t,
                                               @Nonnull String fmt, Object... args) {
        ByteBuf bb = response.alloc().buffer();
        String msg = fmt.isEmpty() ? "" : String.format(fmt, args);
        bb.writeCharSequence(msg, UTF_8);
        if (t != null) {
            StringBuilderWriter sbWriter = new StringBuilderWriter();
            try (PrintWriter pw = new PrintWriter(sbWriter, true)) {
                t.printStackTrace(pw);
            }
            bb.writeCharSequence(sbWriter.toString(), UTF_8);
        }
        response.chunkedTransfer(false)
                .status(status)
                .header(CONTENT_TYPE, "text/plain; charset=utf-8");
        return Mono.just(bb);
    }

    private final class Params {
        final @Nonnull List<String> defGraphURIs = new ArrayList<>();
        final @Nonnull List<String> namedGraphURIs = new ArrayList<>();
        @Nonnull String query = "";

        public Params(@Nonnull HttpServerRequest request) {
            if (request.method() == HttpMethod.GET) {
                parseURI(request.uri());
            } else if (request.method() == HttpMethod.POST) {
                String ct = request.requestHeaders().get(CONTENT_TYPE, QUERY_TYPE);
                Charset reqCharset = MediaType.parse(ct).charset().or(UTF_8);
                String data = request.receive().aggregate().asString(reqCharset).block();
                if (data == null) {
                    throw new RequestException(400, "POST without request body!");
                } else if (request.isFormUrlencoded()) {
                    parse(data, 0);
                } else if (ct.equals(QUERY_TYPE)) {
                    query = unescape(data, 0, data.length());
                } else {
                    throw new RequestException(400, "POST requests body must be either "+
                            FORM_TYPE+" or "+QUERY_TYPE+". Cannot process Content-Type \""+ct+"\"");
                }
            } else {
                throw new RequestException(406, "Method "+request.method()+" is not allowed " +
                        "in SPARQL protocol. Use GET or POST");
            }
        }

        boolean isReasoning() {
            return defGraphURIs.contains(reasoningGraphIRI)
                    || namedGraphURIs.contains(reasoningGraphIRI);
        }

        void parseURI(@Nonnull String uri) {
            int start = uri.indexOf('?');
            parse(uri, start < 0 ? uri.length() : start+1);
        }

        void parse(@Nonnull String string, int start) {
            while (start < string.length()) {
                int eqIdx = string.indexOf('=', start);
                if (eqIdx < 0)
                    throw new RequestException(400, "No = in "+FORM_TYPE);
                int end = string.indexOf('&', eqIdx);
                end = end < 0 ? string.length() : end;
                String key = string.substring(start, eqIdx).trim();
                String value = unescape(string, eqIdx+1, end);
                if (key.equals("query")) query = value;
                else if (key.equals("default-graph-uri")) defGraphURIs.add(value);
                else if (key.equals("named-graph-uri")) namedGraphURIs.add(value);
                start = end+1;
            }
        }

    }

    public @Nonnull Publisher<Void> handle(@Nonnull HttpServerRequest request,
                                           @Nonnull HttpServerResponse response) {
        Publisher<? extends ByteBuf> content;
        try {
            Params params = new Params(request);
            if (params.query.isEmpty()) {
                content = serviceDescription(request, response);
            } else {
                MediaType mt = chooseMediaType(request, resultTypes);
                ChunkedEncoder encoder = encoderRegistry.get(mt);
                if (encoder == null)
                    throw new RequestException(500, "No encoder for " + mt);
                Op query = parseQuery(params);
                Results results = federation.query(query);
                content = encoder.encode(response.alloc(), results,
                                         query.modifiers().ask() != null,
                                          mt.charset().orNull());
                response.chunkedTransfer(true).header(CONTENT_TYPE, mt.toString())
                        .status(HttpResponseStatus.OK);
            }
        } catch (RequestException |SPARQLParseException e) {
            content = createError(response, HttpResponseStatus.BAD_REQUEST,
                    null, "%s", e.getMessage());
        } catch (Throwable t) {
            content = createError(response, HttpResponseStatus.INTERNAL_SERVER_ERROR, t,
                                 "Unexpected exception while processing request");
        }
        return response.send(content);
    }

    @Nonnull private Op parseQuery(Params params) throws SPARQLParseException {
        Op parsed;
        try {
            parsed = parser.parse(params.query);
        } catch (SPARQLParseException e) {
            if (params.query.contains("+"))
                parsed = parser.parse(params.query = params.query.replace('+', ' '));
            else
                throw e;
        }
        if (params.isReasoning())
            parsed.modifiers().add(Reasoning.INSTANCE);
        return parsed;
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

    private @Nonnull Publisher<? extends ByteBuf>
    serviceDescription(@Nonnull HttpServerRequest request, @Nonnull HttpServerResponse response) {
        MediaType mt = chooseMediaType(request, PREFERRED_RDF_TYPES);
        Lang lang = RDFLanguages.contentTypeToLang(mt.type()+"/"+mt.subtype());
        if (lang == null)
            throw new RequestException(500, "Requested RDF syntax not available: "+mt);

        Model model = ModelFactory.createDefaultModel();
        String uri = request.uri().replaceAll("/$", "");
        if (!uri.startsWith("http://") && uri.startsWith("/")) {
            InetSocketAddress a = request.hostAddress();
            if (a != null) {
                String host = request.requestHeaders().get(HttpHeaderNames.HOST, a.getHostString());
                String portString = host.matches(".*:\\d+$") ? "" : ":" + a.getPort();
                uri = "http://" + host + portString + uri;
            }
        }
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

        ByteBuf bb = response.alloc().buffer();
        RDFDataMgr.write(new OutputStream() {
            @Override public void write(int b) {bb.writeByte(b);}
            @Override public void write(@Nonnull byte[] b, int i, int l) {bb.writeBytes(b, i, l);}
        }, model, lang);
        response.header(CONTENT_TYPE, mt.toString()).status(HttpResponseStatus.OK);
        return Mono.just(bb);
    }

    public static class RequestException extends RuntimeException {
        public final int status;
        public RequestException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }

    private static boolean match(@Nonnull String accepted, @Nonnull String offer) {
        return accepted.equals("*") || accepted.equals(offer);
    }
    private static boolean match(@Nonnull MediaType accepted, @Nonnull MediaType offer) {
        return match(accepted.type(), offer.type()) && match(accepted.subtype(), offer.subtype());
    }

    private static @Nullable MediaType tryMatch(@Nonnull List<MediaType> offers,
                                                @Nonnull MediaType accepted,
                                                @Nonnull List<String> acceptedCharsets) {
        MediaType mt = offers.stream().filter(o -> match(accepted, o)).findFirst().orElse(null);
        if (mt == null)
            return null;
        HashMultimap<String, String> params = null;
        for (Map.Entry<String, String> e : accepted.parameters().entries()) {
            if (!e.getKey().equals("q")) {
                if (params == null)
                    params = HashMultimap.create();
                params.put(e.getKey(), e.getValue());
            }
        }
        String acceptedCharset = null;
        for (int i = 0, sz = acceptedCharsets.size(); acceptedCharset == null && i < sz; i++) {
            String name = acceptedCharsets.get(i);
            try {
                if (Charset.isSupported(name)) acceptedCharset = name;
            } catch (IllegalCharsetNameException ignored) { }
        }
        if (params != null) {
            if (!params.containsKey("charset") && acceptedCharset != null)
                params.put("charset", acceptedCharset);
            mt = mt.withParameters(params);
        } else if (acceptedCharset != null) {
            mt = mt.withCharset(Charset.forName(acceptedCharset));
        }
        return mt;
    }

    private @Nonnull MediaType
    chooseMediaType(@Nonnull HttpServerRequest request, @Nonnull List<MediaType> preferred) {
        List<String> hs = new ArrayList<>(request.requestHeaders().getAll(ACCEPT));
        addQueryParamAcceptedTypes(request, hs);
        if (hs.isEmpty())
            hs.add(preferred.get(0).toString());
        List<String> acceptedCharsets = request.requestHeaders()
                .getAll(HttpHeaderNames.ACCEPT_CHARSET).stream()
                .flatMap(s -> Arrays.stream(s.split(", *")))
                .collect(Collectors.toList());
        MediaType mt = hs.stream()
                .flatMap(s -> Arrays.stream(s.split(", *")))
                .map(MediaType::parse)
                .sorted(REV_Q_COMPARATOR)
                .map(accepted -> tryMatch(preferred, accepted, acceptedCharsets))
                .filter(Objects::nonNull).findFirst().orElse(null);
        if (mt != null)
            return mt;
        String offered = preferred.toString().replaceAll("^\\[|]$", "");
        throw new RequestException(406, "Cannot produce any of the media types given " +
                                    "in Accept headers. Supported media types are: "+offered);
    }

    private void addQueryParamAcceptedTypes(@Nonnull HttpServerRequest request, List<String> hs) {
        Matcher matcher = OUT_PARAM_RX.matcher(request.uri());
        while (matcher.find()) {
            String type = unescape(matcher.group(1));
            if (type.indexOf('/') >= 0) {
                try {
                    hs.add(0, MediaType.parse(type).toString());
                } catch (IllegalArgumentException e) {
                    throw new RequestException(400, "Bad output format in query parameter: "+type);
                }
            } else {
                switch (type.trim().toLowerCase()) {
                    case "csv":
                    case "comma-separated-values":
                        hs.add(0, "text/csv");
                        break;
                    case "tsv":
                    case "tab-separated-values":
                        hs.add(0, "text/tab-separated-values");
                        break;
                    case "json":
                        hs.add(0, "application/sparql-results+json");
                        break;
                    case "xml":
                        hs.add(0, "application/sparql-results+xml");
                        break;
                    default:
                        throw new RequestException(400, "Bad output format in query parameter: "+type);
                }
            }
        }
    }
}
