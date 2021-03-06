package br.ufsc.lapesd.freqel.query.endpoint.impl;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.util.DQPushChecker;
import br.ufsc.lapesd.freqel.cardinality.EstimatePolicy;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.NTParseException;
import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.factory.TermFactory;
import br.ufsc.lapesd.freqel.model.term.std.StdTermFactory;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.*;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.DQEndpointException;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.freqel.algebra.Cardinality.*;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class SPARQLClient extends AbstractTPEndpoint implements DQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLClient.class);

    public static final String TSV_TYPE = "text/tab-separated-values";
    public static final String JSON_TYPE = "application/sparql-results+json";

    private static final String TSV_ACCEPT = "text/tab-separated-values; charset=utf-8, " +
                                             "text/tab-separated-values; q=0.9";
    private static final String JSON_ACCEPT = "application/sparql-results+json, " +
                                              "application/json; q=0.9";
    private static final String XSD_STRING_SUFFIX = "^^<"+ V.XSD.xstring.getURI()+">";
    private static final Pattern CSV_URI_RX = Pattern.compile("^\"?\\w+:");
    private static final Pattern DISTINCT_RX = Pattern.compile("(?i)^SELECT\\W*DISTINCT");
    private static final String POST_QUOTE = "@^\t\r\n";

    private final @Nonnull HttpHost host;
    private final @Nonnull String uri;
    private boolean alwaysRestrictAccept = false;
    private @Nullable Map<String, Map<String, String>> mt2params = null;
    private final @Nonnull Map<String, String> globalParams = new HashMap<>();
    private @Nullable Set<Capability> missingCapabilities = null;
    private @Nullable Consumer<HttpClientBuilder> httpClientBuilderConfigurator = null;
    private final @Nonnull WeakHashMap<BaseResults, Boolean> activeResults = new WeakHashMap<>();
    private final @Nonnull PoolingHttpClientConnectionManager connMgr
            = new PoolingHttpClientConnectionManager();
    private final @Nonnull ConcurrentLinkedQueue<CloseableHttpClient> clientPool
            = new ConcurrentLinkedQueue<>();
    private final @Nonnull ThreadPoolExecutor connectExecutor;
    private boolean warnedCSVFormat = false;
    private int fallbackKeepAliveTimeout = 10;
    private long statsLogMs = 5*60*1000;
    private int nQueries, nFailedQueries, nDiscardedSolutions;
    private double createSPARQLMsAvg, createGetMsAvg, createClientMsAvg, responseMsAvg;
    private final @Nonnull Stopwatch statsLogSw = Stopwatch.createStarted();
    private final @Nonnull List<Callable<?>> onCloseCallbacks = new ArrayList<>();

    /* --- --- --- Configuration --- --- --- */

    /**
     * Create a client for the given SPARQL endpoint URI.
     *
     * @param uri the endpoint URI where to send SPARQL requests
     * @throws IllegalArgumentException if uri is not a valid URI
     */
    public SPARQLClient(@Nonnull String uri) throws IllegalArgumentException {
        this(java.net.URI.create(uri));
    }

    public SPARQLClient(@Nonnull URI uri) {
        this.uri = uri.toString();
        this.host = URIUtils.extractHost(uri);
        connMgr.setDefaultMaxPerRoute(128);
        connMgr.setMaxTotal(128);
        this.connectExecutor = new ThreadPoolExecutor(0,
                2, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
        connectExecutor.submit(() -> {}); //dummy to keep an initial thread ready
    }

    public @Nonnull String getURI() {
        return uri;
    }

    /**
     * Run the given callable after an effective {@link SPARQLClient#close()}.
     *
     * Only the first call to {@link SPARQLClient#close()} is effective, thus callbacks
     * run only once. Any exception thrown by the callable will be logged instead of re-thrown.
     *
     * CAllbacks are run in the reverse order in which they were registered (last to be
     * registered runs first). Registration should be done upon initialization of resources
     * to be associated to this {@link SPARQLClient}.
     *
     * @param callable what to do after an effective close
     * @return this {@link SPARQLClient}
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient onClose(@Nonnull Callable<?> callable) {
        onCloseCallbacks.add(callable);
        return this;
    }

    /**
     * @see SPARQLClient#onClose(Callable)
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient onClose(@Nonnull Runnable runnable) {
        return onClose(() -> {runnable.run(); return null;});
    }

    /**
     * Sets the minimum interval between spontaneous logging of performance metrics.
     *
     * The lgging will only occur during {@link SPARQLClient#query},
     * {@link SPARQLClient#querySPARQL(String)} or {@link SPARQLClient#close()}, so the
     * interval can be larger than what is given here.
     *
     * @param interval the interval value
     * @param unit the unit of the interval
     * @return this {@link SPARQLClient}
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient setStatsLogInterval(long interval, @Nonnull TimeUnit unit) {
        this.statsLogMs = TimeUnit.MILLISECONDS.convert(interval, unit);
        return this;
    }

    /**
     * Sets the maximum number of connections to be kept alive (active or idle).
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient setMaxConnections(int maxConnections) {
        connMgr.setMaxTotal(maxConnections);
        connMgr.setDefaultMaxPerRoute(maxConnections);
        return this;
    }

    /**
     * Return a map of parameter names and values to always be included in any GET-based query
     * sent to the remote endpoint.
     *
     * Values in the map are expected to not be percent-encoded.
     *
     * @return a mutable Map from parameter names to their values.
     */
    public @Nonnull Map<String, String> getGlobalParams() {
        return globalParams;
    }

    /**
     * Although this is not standard [1], some endpoints may expect the results format
     * to be defined using query parameters. This sets which are the parameters and
     * values for what would be sent in the Accept header.
     *
     * Default values are null, meaning only the Accept header will be set.
     *
     * [1]: https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/
     *
     * @param type the value of the Accept header
     * @param parameters the equivalent set of parameter=value parameters to be
     *                   included as query parameters, or null if Accept is honored by the server
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient setMediaTypeParameters(@Nonnull String type,
                                                        @Nullable Map<String, String> parameters) {
        if (!JSON_TYPE.equals(type) && !TSV_TYPE.equals(type)) {
            throw new IllegalArgumentException("mediaType must be SPARQLClient.JSON_MEDIA_TYPE " +
                                               "or SPARQLClient.TSV_MEDIA_TYPE, got"+type);
        }
        if (parameters == null && mt2params != null) {
            mt2params.remove(type);
        } else {
            if (mt2params == null) mt2params = new HashMap<>();
            mt2params.put(type, parameters);
        }
        return this;
    }

    /**
     * If {@link SPARQLClient#setMediaTypeParameters(String, Map)} is set for a media type,
     * the client will include "* / *" (without the spaces) as the lowest-priority accepted
     * media type. With <code>setAlwaysSendAccept(true)</code>, Accept will include only the
     * media type and not the accept all alternative.
     *
     * This is seldom useful. Use for particularly transgressive servers.
     */
    @CanIgnoreReturnValue
    public @Nonnull SPARQLClient setAlwaysRestrictAccept(boolean alwaysRestrictAccept) {
        this.alwaysRestrictAccept = alwaysRestrictAccept;
        return this;
    }

    /**
     * When the server allows Keep-Alive but does not set a timeout, this value,
     * in seconds, will be used.
     */
    public @Nonnull SPARQLClient setFallbackKeepAliveTimeout(int fallbackKeepAliveTimeout) {
        this.fallbackKeepAliveTimeout = fallbackKeepAliveTimeout;
        return this;
    }

    /**
     * @see SPARQLClient#setFallbackKeepAliveTimeout(int)
     */
    public @CheckReturnValue int getFallbackKeepAliveTimeout() {
        return fallbackKeepAliveTimeout;
    }

    /**
     * Configures the {@link HttpClientBuilder}.
     *
     * This configuration occurs for every query() call. The configurator is run after
     * {@link SPARQLClient} has applied its own configurations, so it can override defaults.
     *
     * @param configurator the configuration routine or null (to perform no extra configuration).
     */
    public void setHttpClientBuilderConfigurator(@Nullable Consumer<HttpClientBuilder> configurator) {
        this.httpClientBuilderConfigurator = configurator;
    }

    /* --- --- --- Interface Implementation --- --- --- */

    @Override
    public @Nonnull String toString() {
        return "SPARQLClient{"+uri+"}";
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        if (query.attr().isAsk())
            return execute(query, JSON_ACCEPT, emptySet(), AskResults::new);
        Projection p = query.getModifiers().projection();
        Set<String> vars = p == null ? query.attr().publicTripleVarNames() : p.getVarNames();
        return execute(query, TSV_ACCEPT, vars, TSVResults::new);
    }

    @Override public double alternativePenalty(@NotNull CQuery query) {
        return 0.25;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return EMPTY;

        if (EstimatePolicy.canQueryRemote(policy) || EstimatePolicy.canAskRemote(policy)) {
            MutableCQuery askQuery = new MutableCQuery(query);
            if (askQuery.getModifiers().ask() == null)
                askQuery.mutateModifiers().add(Ask.INSTANCE);
            try (Results results = query(askQuery)) {
                return results.hasNext() ? NON_EMPTY : EMPTY;
            } catch (QueryExecutionException e) { return EMPTY; }
        }
        return UNSUPPORTED;
    }

    public void removeRemoteCapability(@Nonnull Capability capability) {
        if (missingCapabilities == null)
            missingCapabilities = new HashSet<>();
        missingCapabilities.add(capability);
    }

    @Override
    public boolean hasSPARQLCapabilities() {
        return missingCapabilities == null;
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        if (missingCapabilities != null && missingCapabilities.contains(capability))
            return false;
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case CARTESIAN:
            case LIMIT:
            case SPARQL_FILTER:
            case VALUES:
            case OPTIONAL:
            case ASK:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean canQuerySPARQL() {
        return true;
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        Query parsed = QueryFactory.create(sparqlQuery);
        return querySPARQL(sparqlQuery, parsed.isAskType(), parsed.getResultVars());
    }

    @Override
    public @Nonnull Results querySPARQL(@Nonnull String sparqlQuery, boolean isAsk,
                                         @Nonnull Collection<String> vars) {
        String accept = isAsk ? JSON_ACCEPT : TSV_ACCEPT;
        Connection connection = new Connection(sparqlQuery, isAsk, accept);
        Future<Connection> future = connectExecutor.submit(connection);
        BaseResults results = isAsk ? new AskResults(vars, future) : new TSVResults(vars, future);
        synchronized (this) {
            activeResults.put(results, true);
        }
        return results;
    }

    @Override
    public @Nonnull DisjunctiveProfile getDisjunctiveProfile() {
        return SPARQLDisjunctiveProfile.DEFAULT;
    }

    @Override
    public @Nonnull Results query(@Nonnull Op query) throws DQEndpointException,
                                                            QueryExecutionException {
        assert query.modifiers().stream().allMatch(m -> hasCapability(m.getCapability()));
        assert new DQPushChecker(getDisjunctiveProfile()).setEndpoint(this).canPush(query);
        boolean isAsk = query.modifiers().ask() != null || query.getResultVars().size() == 0;
        String accept = isAsk ? JSON_ACCEPT : TSV_ACCEPT;
        Set<String> vars = query.getResultVars();
        Connection connection = new Connection(query, isAsk, accept);
        Future<Connection> future = connectExecutor.submit(connection);
        BaseResults results = isAsk ? new AskResults(vars, future) : new TSVResults(vars, future);
        results.setOptional(query.modifiers().optional() != null);
        synchronized (this) {
            activeResults.put(results, true);
        }
        return results;
    }

    @Override public boolean ignoresAtoms() {
        return true;
    }

    protected @Nonnull BaseResults execute(@Nonnull CQuery query, @Nonnull String accept,
                                           @Nonnull Set<String> vars,
                                           @Nonnull ResultsFactory resultsFactory) {
        Connection connection = new Connection(query, vars.isEmpty(), accept);
        Future<Connection> future = connectExecutor.submit(connection);
        BaseResults results = resultsFactory.create(vars, future);
        results.setOptional(query.getModifiers().optional() != null);
        synchronized (this) {
            activeResults.put(results, true);
        }
        return results;
    }

    /* --- --- --- Internal utilities --- --- --- */

    protected class Connection implements Callable<Connection> {
        @Nullable CloseableHttpClient httpClient;
        @Nullable HttpClientContext httpContext;
        @Nullable CloseableHttpResponse httpResponse;
        @Nullable HttpGet httpGet;
        @Nullable Reader reader;
        boolean distinct, ask;
        @Nullable Op opQuery;
        @Nullable CQuery query;
        @Nullable String sparqlQuery;

        final @Nonnull String accept;


        public Connection(@Nonnull Op query, boolean isAsk, @Nonnull String accept) {
            this.opQuery = query;
            this.accept = accept;
            this.ask = isAsk;
        }

        public Connection(@Nonnull CQuery query, boolean isAsk, @Nonnull String accept) {
            this.query = query;
            this.accept = accept;
            this.ask = isAsk;
        }

        public Connection(@Nonnull String sparqlQuery, boolean isAsk, @Nonnull String accept) {
            this.sparqlQuery = sparqlQuery;
            this.accept = accept;
            this.ask = isAsk;
        }

        @Override @Contract("-> this")
        public @Nonnull Connection call() throws QueryExecutionException {
            Stopwatch sw = Stopwatch.createStarted();
            if (sparqlQuery == null) {
                assert query != null || opQuery != null;
                SPARQLString ss = opQuery != null ? SPARQLString.create(opQuery)
                                                  : SPARQLString.create(query);
                sparqlQuery = ss.getSparql();
                distinct = ss.isDistinct();
            } else {
                distinct = DISTINCT_RX.matcher(sparqlQuery).find();
            }
            double createSPARQLMs = sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
            sw.reset().start();
            httpClient = createClient();
            httpContext = new HttpClientContext();
            double setupMs = sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
            sw.reset().start();
            try {
                httpGet = createGet(sparqlQuery, accept);
                double createGetMs = sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
                sw.reset().start();
                httpResponse = httpClient.execute(host, httpGet, httpContext);
                assert httpResponse != null;
                double responseMs = sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
                updateTimes(createSPARQLMs, setupMs, createGetMs, responseMs, httpResponse.getStatusLine().getStatusCode());
                if (!httpResponse.getEntity().isStreaming()) {
                    logger.warn("HttpResponse entity for {} is not streaming. " +
                                "This will hurt parallelism", httpGet.getURI());
                }
                logger.debug("{}ms for GET {} ", responseMs, httpGet.getURI());
                Charset cs = ask ? UTF_8 : getCharset(httpResponse, httpContext);
                this.reader = new InputStreamReader(httpResponse.getEntity().getContent(), cs);
                return this;
            } catch (IOException e) {
                throw new QueryExecutionException("IOException while reading from "
                                                  +httpGet.getURI()+": "+e.getMessage());
            }
        }

        public void close() throws Exception {
            Exception exception = null;
            if (httpResponse != null) {
                assert httpContext != null;
                try {
                    releaseConnection(httpResponse, httpContext);
                } catch (Exception e) { exception = e; }
            }

            try {
                if (reader != null) reader.close();
            } catch (IOException e) {
                if (exception == null) exception = e;
                exception.addSuppressed(e);
            }
            try {
                if (httpResponse != null)
                    httpResponse.close();
            } catch (IOException e) {
                if (exception == null) exception = e;
                exception.addSuppressed(e);
            }
            if (httpClient != null)
                clientPool.add(httpClient);
            if (exception != null)
                throw exception;
        }
    }

    private synchronized void updateTimes(double createSPARQLMs, double createClientMs,
                                          double createGetMs, double responseMs, int status) {
        ++nQueries;
        if (status > 299 || status < 200) ++nFailedQueries;
        createSPARQLMsAvg = (createSPARQLMsAvg * (nQueries-1) + createSPARQLMs) / nQueries;
        createClientMsAvg = (createClientMsAvg * (nQueries-1) + createClientMs) / nQueries;
        createGetMsAvg    = (createGetMsAvg    * (nQueries-1) + createGetMs   ) / nQueries;
        responseMsAvg     = (responseMsAvg     * (nQueries-1) + responseMs    ) / nQueries;
        logStats(false);
    }

    private synchronized void logStats(boolean force) {
        if (force || statsLogSw.elapsed(TimeUnit.MILLISECONDS) > statsLogMs) {
            statsLogSw.reset().start();
            logger.info("SPARQLClient({}) {} queries ({} non-200) {} discarded solutions" +
                            ", createSPARQL avg={}ms, createClient avg={}ms" +
                            ", create GET avg={}ms, response avg={}ms",
                    uri, nQueries, nFailedQueries, nDiscardedSolutions,
                    createSPARQLMsAvg, createClientMsAvg, createGetMsAvg, responseMsAvg);
        }
    }

    private @Nonnull CloseableHttpClient createClient() {
        CloseableHttpClient client = clientPool.poll();
        if (client != null)
            return client;
        HttpClientBuilder builder = HttpClientBuilder.create()
                .setConnectionManager(connMgr)
                .setConnectionManagerShared(true);
        if (httpClientBuilderConfigurator != null)
            httpClientBuilderConfigurator.accept(builder);
        return builder.build();
    }

    private void releaseConnection(@Nonnull HttpResponse response, @Nonnull HttpClientContext ctxt) {
        int timeoutSecs = fallbackKeepAliveTimeout;
        Header connHeader = response.getFirstHeader("Connection");
        if (connHeader != null) {
            if (connHeader.getValue().trim().equals("close"))
                return; //not reusable
        }
        Header kaHeader = response.getFirstHeader("Keep-Alive");
        if (kaHeader != null) {
            for (HeaderElement element : kaHeader.getElements()) {
                NameValuePair timeout = element.getParameterByName("timeout");
                if (timeout != null) {
                    try {
                        timeoutSecs = Integer.parseInt(timeout.getValue());
                    } catch (NumberFormatException ignored) {
                        logger.info("Server {} sent malformed Keep-Alive header with timeout={}",
                                    uri, timeout.getValue());
                    }
                }
            }
        }
        connMgr.releaseConnection(ctxt.getConnection(HttpClientConnection.class),null,
                                  timeoutSecs, TimeUnit.SECONDS);
    }

    private @Nonnull HttpGet createGet(@Nonnull String sparql,
                                       @Nonnull String accept) throws QueryExecutionException {
        assert !accept.isEmpty();
        ArrayList<BasicNameValuePair> list = new ArrayList<>();
        list.add(new BasicNameValuePair("query", sparql));
        globalParams.forEach((k, v) -> list.add(new BasicNameValuePair(k, v)));
        if (mt2params != null) {
            Map<String, String> params = mt2params.getOrDefault(accept, emptyMap());
            //if using params, be flexible and accept anything (but prefer mediaType)
            if (!params.isEmpty() && !alwaysRestrictAccept)
                accept = accept + ", */*; q=0.7";
            params.forEach((k, v) -> list.add(new BasicNameValuePair(k, v)));
        }
        String fullUri = uri + "?" + URLEncodedUtils.format(list, '&', UTF_8);
        URI parsedUri;
        try {
            parsedUri = URI.create(fullUri);
        } catch (IllegalArgumentException e) {
            String msg = String.format("Invalid URI %s created for query %s", fullUri, sparql);
            throw new QueryExecutionException(msg, e);
        }
        HttpGet get = new HttpGet(parsedUri);
        get.setHeader("Accept", accept);
        get.setHeader("Connection", "Keep-Alive");
        return get;
    }

    private static Charset getCharset(@Nonnull HttpResponse response,
                                      @Nonnull HttpClientContext context) {
        String charsetName = "UTF-8";
        for (HeaderElement element : response.getEntity().getContentType().getElements()) {
            NameValuePair pair = element.getParameterByName("charset");
            if (pair != null)
                charsetName = pair.getValue();
        }
        try {
            return Charset.forName(charsetName.toUpperCase());
        } catch (IllegalCharsetNameException e) {
            logger.error("Invalid charset {} for URI {}. Will assume UTF-8",
                         charsetName, ((HttpGet)context.getRequest()).getURI());
        }
        return UTF_8;
    }

    /* --- --- --- Results & connections management --- --- --- */

    @Override
    public void close() {
        ArrayList<BaseResults> victims;
        synchronized (this) {
            victims = new ArrayList<>(activeResults.keySet());
            activeResults.clear();
        }
        for (BaseResults victim : victims) {
            try {
                victim.close();
            } catch (ResultsCloseException e) {
                logger.error("SPARQLClient[{}].close() ignoring error on Results.close", uri, e);
            }
        }
        connectExecutor.shutdown();
        try {
            if (!connectExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                logger.error("{}'s connectExecutor is taking too long to terminate, " +
                             "will stop waiting", this);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (CloseableHttpClient c = clientPool.poll(); c  != null; c = clientPool.poll()) {
            try {
                c.close();
            } catch (IOException e) {
                logger.error("SPARQLClient({}).close():  ignoring pooled HttpClient close error",
                             uri, e);
            }
        }
        connMgr.close();

        ListIterator<Callable<?>> it = onCloseCallbacks.listIterator(onCloseCallbacks.size());
        while (it.hasPrevious()) {
            Callable<?> callback = it.previous();
            try {
                callback.call();
            } catch (Exception e) {
                logger.error("SPARQLClient({}).close() callback {} failed.", uri,  callback, e);
            }
        }
        logStats(true);
    }

    @FunctionalInterface
    protected interface ResultsFactory {
        @Nonnull BaseResults create(@Nonnull Set<String> vars,
                                    @Nonnull Future<Connection> future);
    }

    protected abstract class BaseResults extends AbstractResults {
        protected final @Nonnull Queue<Solution> queue = new ArrayDeque<>();
        protected final @Nonnull Future<Connection> connectionFuture;
        protected @Nullable Connection connection;
        protected @Nullable Exception connectionFailure;
        protected boolean closed = false, exhausted = false, distinct = false;

        protected BaseResults(@Nonnull Collection<String> varNames, @Nonnull Future<Connection> connectionFuture) {
            super(varNames);
            this.connectionFuture = connectionFuture;
        }

        protected abstract void parse(int minimumSolutions, int millisecondsTimeout);

        protected boolean waitForConnection(int timeoutMilliseconds) {
            boolean interrupted = false;
            while (connection == null && connectionFailure == null) {
                try {
                    connection = connectionFuture.get(timeoutMilliseconds, TimeUnit.MILLISECONDS);
                    distinct = connection.distinct;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    logger.error("Error reading from server: {}", e.getCause().getMessage());
                    connectionFailure = (Exception) e.getCause();
                } catch (TimeoutException e) {
                    return false; //timeout -- do not change state (connection, connectionFailure)
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            assert connection != null || connectionFailure != null;
            return connection != null; //ok if has connection, not ok if has connectionFailure
        }

        @Override
        public int getReadyCount() {
            return queue.size();
        }

        @Override
        public boolean isAsync() {
            return true;
        }

        @Override
        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public boolean hasNext() {
            return hasNext(Integer.MAX_VALUE);
        }
        @Override
        public boolean hasNext(int millisecondsTimeout) {
            if (!waitForConnection(millisecondsTimeout))
                return false; // timeout or failed to init: no results
            if (!queue.isEmpty())
                return true; // has buffered Solutions
            if (!exhausted)
                parse(1, millisecondsTimeout);
            return !queue.isEmpty();
        }

        @Override
        public @Nonnull Solution next() {
            if (!hasNext())
                throw new NoSuchElementException();
            assert !queue.isEmpty();
            return queue.remove();
        }

        @Override @OverridingMethodsMustInvokeSuper
        public void close() throws ResultsCloseException {
            if (closed)
                return; //no work
            closed = true;
            exhausted = true; //avoid further parse() calls

            synchronized (SPARQLClient.this) {
                activeResults.remove(this);
            }
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                throw new ResultsCloseException(this, e);
            }
        }
    }

    protected class TSVResults extends BaseResults {
        private @Nullable ArraySolution.ValueFactory solutionFac;
        private final @Nonnull StringBuilder line = new StringBuilder();
        private final @Nonnull BitSet includedColumns = new BitSet();
        private int nullColumnsCount = 0;
        private int records = -1;
        private boolean csvFormat = false, gotReturn = false, inQuotes = false, inTerm = false;
        private int innerQuotes = 0;
        private final @Nonnull TermFactory termFactory = new StdTermFactory();

        public TSVResults(@Nonnull Collection<String> varNames,
                          @Nonnull Future<Connection> connectionFuture) {
            super(varNames, connectionFuture);

        }

        private @Nullable URI getURI() {
            assert connection == null || connection.httpGet != null;
            return connection == null ? null : connection.httpGet.getURI();
        }

        @CanIgnoreReturnValue
        private boolean parseChar(char c) {
            if (c == '"') {
                if (!inTerm) {
                    inQuotes = true;
                } else {
                    ++innerQuotes;
                    if (innerQuotes == 2)
                        innerQuotes = 0;
                    assert innerQuotes < 2;
                }
            } else if (inQuotes && innerQuotes == 1) {
                innerQuotes = 0;
                if (POST_QUOTE.indexOf(c) < 0) {
                    logger.error("Bad closing \" mark. Closes a term but is followed by " +
                                 "'{}' (int value: {}). Current line buffer: {}", c,
                                 Character.getNumericValue(c), line.toString());
                } else {
                    inQuotes = false; //effectively close the quotation
                }
            } else {
                assert innerQuotes == 0;
            }
            inTerm = true;

            if (c == '\r' && !inQuotes) {
                if (gotReturn)
                    line.append('\r');
                gotReturn = true;
            } else if (c == '\n' && !inQuotes) {
                boolean parsed = parseLine();
                gotReturn = false;
                inTerm = false;
                return parsed; //true if queued a new solution
            } else {
                if (c == '\t' && !inQuotes)
                    inTerm = false;
                gotReturn = false;
                line.append(c);
            }
            return false; //did not queued a new solution
        }

        private boolean parseLine() {
            if (records == -1) {
                parseHeaders();
                records = 0;
            } else {
                List<Term> terms = new ArrayList<>();
                int idx = -1;
                for (String ntString : Splitter.on('\t').split(line.toString())) {
                    ++idx;
                    if (!includedColumns.get(idx))
                        continue; // column is projected-out
                    try {
                        terms.add(parseTerm(ntString));
                    } catch (NTParseException e) {
                        logger.error("Discarding record {} due to invalid NT string: {}.",
                                     records, ntString);
                        ++nDiscardedSolutions;
                        terms = null;
                        break;
                    }
                }
                line.setLength(0);
                ++records;
                if (terms != null) {
                    assert solutionFac != null;
                    for (int i = 0; i < nullColumnsCount; i++) terms.add(null);
                    queue.add(solutionFac.fromValues(terms));
                    return true;
                }
            }
            return false;
        }

        private @Nullable Term parseTerm(@Nonnull String nt) throws NTParseException {
            try {
                return RDFUtils.fromNT(unquote(nt), termFactory);
            } catch (NTParseException e) {
                if (!csvFormat && (CSV_URI_RX.matcher(nt).find()
                                || (nt.charAt(0) == '"' && nt.charAt(nt.length()-1) == '"')) ) {
                    setCsvFormat(); //late csv-quoting detected
                    try {
                        return RDFUtils.fromNT(unquote(nt), termFactory);
                    } catch (NTParseException ignored) { }
                }
                throw e; //could not recover
            }
        }

        /**
         * Handle Bad/old server sending bad TSV using rules for CSV.
         *
         * CSV encoding rules loose important information. This lost information will be guessed.
         * In most cases the guess is correct, but if the real data had literals that looked
         * like URIs, they wil be guessed here to be URIs and if the original data had
         * relative URIs, they will be guessed as literals here. Anything that is guessed to
         * be a literal becomes xsd:string.
         */
        private @Nonnull String unquote(@Nonnull String nt) {
            if (nt.isEmpty()) return nt;
            char first = nt.charAt(0), last = nt.charAt(nt.length() - 1);
            if (!csvFormat) {
                if (first == '"' && last == '"') {
                    // unpack from the ""
                    String inner = nt.substring(1, nt.length() - 1).replace("\\\"", "\"");
                    // there is an ambiguity: plain literals matched the previous if and
                    // using inner would fail the parse. Thus we need to look inside inner
                    // and determine if it is a
                    //  - typed literal,
                    //  - lang literal,
                    //  - plain literal, or
                    //  - URI
                    // If it is none of these, nt itself was a plain literal and we return nt

                    first = inner.charAt(0);
                    last = inner.charAt(inner.length()-1);
                    boolean ok = (first == '<' && last == '>')            // uri
                            || (first == '"' && last == '>')              // typed literal
                            || (first == '"' && Character.isLetter(last)) // lang literal
                            || (first == '"' && last == '"' );            // plain literal
                    return ok ? inner : nt;
                }
                return nt;
            } else {
                // CSV encoding: rdf node type information is discarded
                if (first == '<' && last == '>') { // buggy server: should not occur in CSV
                    return nt; // a NT-formatted URI
                } else if (first == '"' && last != '"') { // buggy server: should not occur in CSV
                    return nt; // a NT-formatted typed or lang literal
                } else {
                    if (CSV_URI_RX.matcher(nt).find()) { // looks like an absolute URI
                        if (first == '"')
                            return "<" + nt.substring(1, nt.length() - 1) + ">";
                        return "<" + nt + ">";
                    } else { //else: treat as plain literal
                        if (first != '"')
                            return "\"" + nt + "\"" + XSD_STRING_SUFFIX;
                        return nt + XSD_STRING_SUFFIX;
                    }
                }
            }
        }

        private void parseHeaders() {
            List<String> actual = new ArrayList<>();
            int idx = -1;
            for (String var : Splitter.on('\t').split(line.toString())) {
                ++idx;
                // both TSV and CSV can quote terms
                if (var.charAt(0) == '"' && var.charAt(var.length()-1) == '"')
                    var = var.substring(1, var.length() - 1);
                if (var.charAt(0) == '?' || var.charAt(0) == '$') {
                    var = var.substring(1);
                } else {
                    // TSV format requires ?, while CSV forbids
                    // this hints about how the term values are encoded. CSV formatting
                    // rules loose information, so it should be avoided
                    setCsvFormat();
                }
                if (varNames.contains(var)) {
                    includedColumns.set(idx);
                    actual.add(var);
                }
            }
            if (new HashSet<>(actual).size() < actual.size()) {
                logger.error("{} sent duplicate variables in response: {}. URI: {}",
                             host, actual, getURI());
            }
            nullColumnsCount = varNames.size() - actual.size();
            assert nullColumnsCount >= 0;
            if (nullColumnsCount > 0) {
                Set<String> missing = CollectionUtils.setMinus(varNames, actual);
                logger.warn("{} omitted variables {} in response to {}", host, missing, getURI());
                assert missing.stream().noneMatch(actual::contains);
                actual.addAll(missing);
            }
            solutionFac = ArraySolution.forVars(actual);
            line.setLength(0);
        }

        private void setCsvFormat() {
            csvFormat = true;
            if (!warnedCSVFormat) {
                warnedCSVFormat = true;
                logger.warn("Server {} is using CSV formatting rules in {}. CSV rules for RDF " +
                            "term serialization do not distinguish literals from URIs.",
                            uri, TSV_TYPE);
            }
        }

        @Override
        protected void parse(int minimumSolutions, int msTimeout) {
            if (exhausted) return; // no work
            assert connection != null;
            Reader reader = connection.reader;
            assert reader != null;
            assert minimumSolutions >= 0;
            int parsedCount = 0;

            Stopwatch sw = Stopwatch.createStarted();
            try {
                while ((parsedCount < minimumSolutions)) {
                    if (!reader.ready() && sw.elapsed(TimeUnit.MILLISECONDS) >= msTimeout)
                        break;
                    int value = reader.read();
                    if (value >= 0) {
                        if (parseChar((char) value))
                            ++parsedCount;
                    } else {
                        if (line.length() > 0) { //add missing separator
                            parseChar('\r');
                            if (parseChar('\n'))
                                ++parsedCount;
                        }
                        exhausted = true;
                        break;
                    }
                }
            } catch (IOException e) {
                logger.error("IOException reading results from {}. Will stop fetching " +
                             "additional results. {} ready solutions in the queue",
                             getURI(), queue.size(), e);
                exhausted = true;
            }
        }
    }

    protected class AskResults extends BaseResults {
        public AskResults(@Nonnull Collection<String> vars, @Nonnull Future<Connection> connectionFuture) {
            super(emptySet(), connectionFuture);
            assert vars.isEmpty();
        }

        @Override
        protected void parse(int ignored, int ignoredMillisecondsTimeout) {
            if (exhausted)
                return; // no work
            assert connection != null;
            assert connection.reader != null;
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>)
                        new Gson().fromJson(connection.reader, LinkedTreeMap.class);
                boolean result = parseBoolean(map.getOrDefault("boolean", false).toString());
                if (result)
                    queue.add(ArraySolution.EMPTY);
            } catch (JsonIOException e) {
                logger.error("Problem reading from the connection to host {}. " +
                             "AskResults will return negative.", host, e);
            } catch (JsonSyntaxException e) {
                logger.error("Syntax error in JSON for ASK query at URI {}",
                             requireNonNull(connection.httpGet).getURI());
            } finally {
                exhausted = true;
                close(); // release resources
            }
        }
    }
}
