package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.IndexedParam;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.HTTPRequestInfo;
import br.ufsc.lapesd.riefederator.webapis.requests.HTTPRequestObserver;
import br.ufsc.lapesd.riefederator.webapis.requests.MissingAPIInputsException;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.NoPagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.ResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.TermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.JenaResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.SimpleTermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimitsRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.uri.UriTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class UriTemplateExecutor implements APIRequestExecutor {
    private static final Logger logger = LoggerFactory.getLogger(UriTemplateExecutor.class);

    protected final @SuppressWarnings("Immutable") @Nonnull UriTemplate template;
    protected final @Nonnull ImmutableSet<String> required, optional, missing;
    private final @SuppressWarnings("Immutable") @Nonnull ImmutableMap<String, TermSerializer> input2serializer;
    protected final @SuppressWarnings("Immutable") @Nonnull ClientConfig clientConfig;
    private final @Nonnull ResponseParser parser;
    private final @Nonnull PagingStrategy pagingStrategy;
    private final @SuppressWarnings("Immutable") @Nonnull RateLimitsRegistry rateLimitsRegistry;
    private final @Nonnull ImmutableMap<String, String> indexedNAValues;
    private final @Nonnull ImmutableMap<String, String> listSeparator;
    private @SuppressWarnings("Immutable") @Nonnull HTTPRequestObserver requestObserver;
    private final @Nonnull Client client;

    public UriTemplateExecutor(@Nonnull UriTemplate template, @Nonnull ImmutableSet<String> required,
                               @Nonnull ImmutableSet<String> optional,
                               @Nonnull ImmutableSet<String> missing,
                               @Nonnull Map<String, TermSerializer> input2serializer,
                               @Nonnull ClientConfig clientConfig,
                               @Nonnull ResponseParser parser,
                               @Nonnull PagingStrategy pagingStrategy,
                               @Nonnull RateLimitsRegistry rateLimitsRegistry,
                               @Nonnull ImmutableMap<String, String> indexedNAValues,
                               @Nonnull ImmutableMap<String, String> listSeparator,
                               @Nonnull HTTPRequestObserver requestObserver) {
        this.template = template;
        this.required = required;
        this.optional = optional;
        this.missing = missing;
        this.input2serializer = ImmutableMap.copyOf(input2serializer);
        this.clientConfig = clientConfig;
        this.client = ClientBuilder.newClient(clientConfig);
        this.parser = parser;
        this.parser.setupClient(client);
        this.pagingStrategy = pagingStrategy;
        this.rateLimitsRegistry = rateLimitsRegistry;
        this.indexedNAValues = indexedNAValues;
        this.listSeparator = listSeparator;
        this.requestObserver = requestObserver;
    }

    public UriTemplateExecutor(@Nonnull UriTemplate template) {
        this(template, ImmutableSet.copyOf(template.getTemplateVariables()),
                ImmutableSet.of(), ImmutableSet.of(),
                ImmutableMap.of(), new ClientConfig(),
                JenaResponseParser.INSTANCE, NoPagingStrategy.INSTANCE,
                new RateLimitsRegistry(), ImmutableMap.of(), ImmutableMap.of(), i -> {});
    }

    public static class Builder {
        private final @Nonnull UriTemplate template;
        private Set<String> required = null;
        private Set<String> optional = null;
        private Set<String> missing = null;
        private final @Nonnull Map<String, String> indexedNAValues = new HashMap<>();
        private final @Nonnull ImmutableMap.Builder<String, String> listSeparator
                = ImmutableMap.builder();
        private final @Nonnull ImmutableMap.Builder<String, TermSerializer> input2serializer
                = ImmutableMap.builder();
        private @Nonnull ClientConfig clientConfig = new ClientConfig();
        private @Nonnull ResponseParser parser = JenaResponseParser.INSTANCE;
        private @Nonnull PagingStrategy pagingStrategy = NoPagingStrategy.INSTANCE;
        private @Nonnull RateLimitsRegistry rateLimitsRegistry = new RateLimitsRegistry();
        private @Nonnull HTTPRequestObserver requestObserver = i -> {};

        public Builder(@Nonnull UriTemplate template) {
            this.template = template;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder withRequired(@Nonnull Collection<String> names) {
            if (required == null)
                required = new HashSet<>();
            required.addAll(names);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withRequired(@Nonnull String... names) {
            return withRequired(Arrays.asList(names));
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withOptional(@Nonnull Collection<String> names) {
            if (optional == null)
                optional = new HashSet<>();
            optional.addAll(names);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withOptional(@Nonnull String... names) {
            return withOptional(Arrays.asList(names));
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withMissingInResult(@Nonnull Collection<String> names) {
            if (missing == null)
                missing = new HashSet<>();
            missing.addAll(names);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withMissingInResult(@Nonnull String... names) {
            return withMissingInResult(Arrays.asList(names));
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder withClientConfig(@Nonnull ClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withResponseParser(@Nonnull ResponseParser parser) {
            this.parser = parser;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withSerializer(@Nonnull String input,
                                               @Nonnull TermSerializer serializer) {
            input2serializer.put(input, serializer);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withSerializers(@Nonnull Map<String, TermSerializer> map) {
            map.forEach(this::withSerializer);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withPagingStrategy(@Nonnull PagingStrategy pagingStrategy) {
            this.pagingStrategy = pagingStrategy;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withRateLimitsRegistry(@Nonnull RateLimitsRegistry registry) {
            this.rateLimitsRegistry = registry;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder setRequestObserver(@Nonnull HTTPRequestObserver observer) {
            this.requestObserver = observer;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withIndexedNAValue(@Nonnull String paramBaseName,
                                                   @Nullable String value) {
            if (value == null)
                this.indexedNAValues.remove(paramBaseName);
            else
                this.indexedNAValues.put(paramBaseName, value);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withListSeparator(@Nonnull String paramBaseName,
                                                  @Nonnull String separator) {
            this.listSeparator.put(paramBaseName, separator);
            return this;
        }

        @CheckReturnValue
        public @Nonnull UriTemplateExecutor build() {
            if (!pagingStrategy.getParametersUsed().isEmpty()) {
                if (optional == null)
                    optional = ImmutableSet.copyOf(pagingStrategy.getParametersUsed());
                else
                    optional.addAll(pagingStrategy.getParametersUsed());
                if (required != null)
                    required.removeAll(pagingStrategy.getParametersUsed());
            }
            if (optional == null)
                optional = ImmutableSet.of();
            if (required == null) {
                required = new HashSet<>(template.getTemplateVariables());
                required.removeAll(optional);
            }
            if (missing == null)
                missing = ImmutableSet.of();
            checkArgument(missing.stream().allMatch(n -> optional.contains(n)
                                                      || required.contains(n)),
                          "Some inputs declared missing in results are not optional nor required");
            checkArgument(required.stream().noneMatch(optional::contains),
                          "Some required inputs are also declared optional");
            return new UriTemplateExecutor(template, ImmutableSet.copyOf(required),
                                           ImmutableSet.copyOf(optional),
                                           ImmutableSet.copyOf(missing),
                                           input2serializer.build(), clientConfig,
                                           parser, pagingStrategy, rateLimitsRegistry,
                                           ImmutableMap.copyOf(indexedNAValues),
                                           listSeparator.build(),
                                           requestObserver);
        }
    }

    public static @Nonnull Builder from(@Nonnull UriTemplate template) {
        return new Builder(template);
    }

    @Override
    public @Nonnull ImmutableSet<String> getRequiredInputs() {
        return required;
    }

    @Override
    public @Nonnull ImmutableSet<String> getOptionalInputs() {
        return optional;
    }

    @Override
    public @Nonnull Set<String> getInputsMissingInResult() {
        return missing;
    }

    @Override
    public @Nonnull Iterator<? extends CQEndpoint> execute(@Nonnull Solution input)
            throws APIRequestExecutorException {
        PagingStrategy.Pager pager = pagingStrategy.createPager();
        return new Iterator<CQEndpoint>() {
            @Override
            public boolean hasNext() {
                return !pager.atEnd();
            }

            @Override
            public @Nullable CQEndpoint next() {
                if (!hasNext()) throw new NoSuchElementException();

                Stopwatch sw = Stopwatch.createStarted();
                String uri = getUri(pager.apply(input));
                HTTPRequestInfo info = new HTTPRequestInfo("GET", uri)
                        .setCreateUriMs(sw);

                sw.reset().start();
                WebTarget target = client.target(uri);
                info.setSetupMs(sw);

                Response[] response = {null};
                Object[] obj = {null};
                rateLimitsRegistry.get(uri).request(() -> {
                    try {
                        info.setRequestDate(new Date());
                        sw.reset().start();
                        response[0] = target.request(parser.getAcceptable()).get();
                        info.setStatus(response[0].getStatus());
                        pager.notifyResponse(response[0]);
                        obj[0] = response[0].readEntity(parser.getDesiredClass());
                        info.setRequestMs(sw);
                        if (response[0].getMediaType() != null)
                            info.setContentType(response[0].getMediaType().toString());
                        info.setResponseBytes(response[0].getLength());
                    } catch (RuntimeException e) {
                        info.setException(e);
                        throw e;
                    }
                });
                sw.reset().start();
                CQEndpoint endpoint;
                try {
                    endpoint = parser.parse(obj[0], uri, info);
                    info.setParseMs(sw);
                } catch (RuntimeException e) {
                    info.setException(e);
                    throw e;
                }
                pager.notifyResponseEndpoint(endpoint);

                logger.info(info.toString());
                requestObserver.accept(info);
                return endpoint;
            }
        };
    }

    @Override
    public @Nonnull HTTPRequestObserver setObserver(@Nonnull HTTPRequestObserver observer) {
        HTTPRequestObserver old = this.requestObserver;
        this.requestObserver = observer;
        return old;
    }

    @Override
    public @Nonnull String toString() {
        return template.getTemplate();
    }

    @Override public void close() {
        client.close();
    }

    private class Bindings {
        final Map<String, String> singleValues = new HashMap<>();
        final Map<String, List<String>> multiValues = new HashMap<>();

        public Bindings(@Nonnull Solution input) {
            input.forEach((name, term) -> {
                IndexedParam indexed = IndexedParam.parse(name);
                String baseName = indexed == null ? name : indexed.base;
                String serialized = toValue(baseName, term);
                if (indexed == null)
                    singleValues.put(name, serialized);
                else
                    saveIndexedValue(indexed, serialized);
            });
            compressMultiValues();
        }

        private void compressMultiValues() {
            StringBuilder builder = new StringBuilder();
            for (Iterator<String> it = multiValues.keySet().iterator(); it.hasNext(); ) {
                String baseName = it.next();
                List<String> list = multiValues.get(baseName);
                String separator = urlEncode(listSeparator.getOrDefault(baseName, null));
                String singleton = null;
                for (String value : list) {
                    if (value != null) {
                        if (singleton == null) singleton = value;
                        else                     singleton = null;
                        if (separator != null)
                            builder.append(value).append(separator);
                    }
                }
                if (singleton != null) { //effectively singleValues, handle as such
                    singleValues.put(baseName, singleton);
                    it.remove();
                } else if (builder.length() > 0) { // convert to singleValues using separator
                    assert separator != null;
                    builder.setLength(builder.length()-separator.length());
                    singleValues.put(baseName, builder.toString());
                    builder.setLength(0);
                    it.remove();
                }
            }
        }

        private void saveIndexedValue(@Nonnull IndexedParam indexed, @Nonnull String serialized) {

            List<String> list = multiValues.getOrDefault(indexed.base, null);
            if (list == null) {
                String na = indexedNAValues.getOrDefault(indexed.base, null);
                multiValues.put(indexed.base, list = new ArrayList<>(indexed.size));
                for (int i = 0; i < indexed.size; i++) list.add(na);
            }
            int position = indexed.getIndexValue();
            if (position >= list.size()) {
                assert false : ".size mismatch among indexed bindings of "+indexed.base;
                // if asserts disabled, fix instead of blowing up
                String na = indexedNAValues.getOrDefault(indexed.base, null);
                for (int i = list.size(); i <= position; i++) list.add(na);
            }
            list.set(position, serialized);
        }

        private @Nonnull String toValue(@Nonnull String baseName, @Nonnull Term term) {
            TermSerializer serializer;
            serializer = input2serializer.getOrDefault(baseName, SimpleTermSerializer.INSTANCE);
            UriTemplateExecutor executor = UriTemplateExecutor.this;
            String serialized = serializer.toString(term, baseName, executor);
            return urlEncode(serialized);
        }

        private String urlEncode(String serialized) {
            if (serialized == null) return null;
            try {
                return URLEncoder.encode(serialized, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 is not a valid encoding!?");
            }
        }
    }

    private static boolean isValidURI(@Nonnull String uri) {
        try {
            new java.net.URI(uri);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private @Nonnull String getUri(@Nonnull Solution input) throws APIRequestExecutorException {
        Set<String> missing = IndexedParam.getMissing(getRequiredInputs(), input.getVarNames());
        if (!missing.isEmpty())
            throw new MissingAPIInputsException(missing, this);
        Bindings bindings = new Bindings(input);
        String uri = template.createURI(bindings.singleValues);
        assert isValidURI(uri);
        if (bindings.multiValues.isEmpty())
            return uri;
        StringBuilder builder = new StringBuilder(uri);
        boolean first = uri.matches("\\?[^/]+$");
        for (Map.Entry<String, List<String>> e : bindings.multiValues.entrySet()) {
            for (String value : e.getValue()) {
                builder.append(first ? '?' : '&').append(e.getKey()).append('=').append(value);
                if (first) first = false;
            }
        }
        uri = builder.toString();
        assert isValidURI(uri);
        return uri;
    }
}
