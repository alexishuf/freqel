package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.Solution;
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
import java.net.URLEncoder;
import java.util.*;

@Immutable
public class UriTemplateExecutor implements APIRequestExecutor {
    private static final Logger logger = LoggerFactory.getLogger(UriTemplateExecutor.class);

    protected final @SuppressWarnings("Immutable") @Nonnull UriTemplate template;
    protected final @Nonnull ImmutableSet<String> required, optional;
    private final @SuppressWarnings("Immutable") @Nonnull ImmutableMap<String, TermSerializer> input2serializer;
    protected final @SuppressWarnings("Immutable") @Nonnull ClientConfig clientConfig;
    private final @Nonnull ResponseParser parser;
    private final @Nonnull PagingStrategy pagingStrategy;
    private final @SuppressWarnings("Immutable") @Nonnull RateLimitsRegistry rateLimitsRegistry;
    private @SuppressWarnings("Immutable") @Nonnull HTTPRequestObserver requestObserver;

    public UriTemplateExecutor(@Nonnull UriTemplate template, @Nonnull ImmutableSet<String> required,
                               @Nonnull ImmutableSet<String> optional,
                               @Nonnull Map<String, TermSerializer> input2serializer,
                               @Nonnull ClientConfig clientConfig,
                               @Nonnull ResponseParser parser,
                               @Nonnull PagingStrategy pagingStrategy,
                               @Nonnull RateLimitsRegistry rateLimitsRegistry,
                               @Nonnull HTTPRequestObserver requestObserver) {
        this.template = template;
        this.required = required;
        this.optional = optional;
        this.input2serializer = ImmutableMap.copyOf(input2serializer);
        this.clientConfig = clientConfig;
        this.parser = parser;
        this.pagingStrategy = pagingStrategy;
        this.rateLimitsRegistry = rateLimitsRegistry;
        this.requestObserver = requestObserver;
    }

    public UriTemplateExecutor(@Nonnull UriTemplate template) {
        this(template, ImmutableSet.copyOf(template.getTemplateVariables()), ImmutableSet.of(),
                ImmutableMap.of(), new ClientConfig(),
                JenaResponseParser.INSTANCE, NoPagingStrategy.INSTANCE,
                new RateLimitsRegistry(), i -> {});
    }

    public static class Builder {
        private final @Nonnull UriTemplate template;
        private Set<String> required = null;
        private Set<String> optional = null;
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
            return new UriTemplateExecutor(template, ImmutableSet.copyOf(required),
                                           ImmutableSet.copyOf(optional),
                                           input2serializer.build(), clientConfig,
                                           parser, pagingStrategy, rateLimitsRegistry,
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
                Client client = createClient();
                parser.setupClient(client);
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

    protected @Nonnull Client createClient() {
        return ClientBuilder.newClient(clientConfig);
    }

    protected @Nonnull String getUri(@Nonnull Solution input) throws APIRequestExecutorException {
        Map<String, String> bindings = new HashMap<>();
        input.forEach((name, term) -> {
            TermSerializer serializer;
            serializer = input2serializer.getOrDefault(name, SimpleTermSerializer.INSTANCE);
            String serialized = serializer.toString(term, name, this);
            try {
                bindings.put(name, URLEncoder.encode(serialized, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 is not a valid encoding!?");
            }
        });
        if (!bindings.keySet().containsAll(getRequiredInputs())) {
            Set<String> missing = new HashSet<>(getRequiredInputs());
            missing.removeAll(bindings.keySet());
            throw new MissingAPIInputsException(missing, this);
        }
        return template.createURI(bindings);
    }
}
