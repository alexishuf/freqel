package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.webapis.requests.*;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.paging.NoPagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.parsers.JenaResponseParser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.uri.UriTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.*;

@Immutable
public class UriTemplateExecutor implements APIRequestExecutor {
    protected final @SuppressWarnings("Immutable") @Nonnull UriTemplate template;
    protected final @Nonnull ImmutableSet<String> required, optional;
    private final @SuppressWarnings("Immutable") @Nonnull ImmutableMap<String, TermSerializer> input2serializer;
    protected final @SuppressWarnings("Immutable") @Nonnull ClientConfig clientConfig;
    private final @Nonnull ResponseParser parser;
    private final @Nonnull PagingStrategy pagingStrategy;

    public UriTemplateExecutor(@Nonnull UriTemplate template, @Nonnull ImmutableSet<String> required,
                               @Nonnull ImmutableSet<String> optional,
                               @Nonnull Map<String, TermSerializer> input2serializer,
                               @Nonnull ClientConfig clientConfig,
                               @Nonnull ResponseParser parser,
                               @Nonnull PagingStrategy pagingStrategy) {
        this.template = template;
        this.required = required;
        this.optional = optional;
        this.input2serializer = ImmutableMap.copyOf(input2serializer);
        this.clientConfig = clientConfig;
        this.parser = parser;
        this.pagingStrategy = pagingStrategy;
    }

    public UriTemplateExecutor(@Nonnull UriTemplate template) {
        this(template, ImmutableSet.copyOf(template.getTemplateVariables()), ImmutableSet.of(),
                ImmutableMap.of(), new ClientConfig(),
                JenaResponseParser.INSTANCE, NoPagingStrategy.INSTANCE);
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

        public Builder(@Nonnull UriTemplate template) {
            this.template = template;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder withRequired(@Nonnull String... names) {
            if (required == null)
                required = new HashSet<>();
            required.addAll(Arrays.asList(names));
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder withOptional(@Nonnull String... names) {
            if (optional == null)
                optional = new HashSet<>();
            optional.addAll(Arrays.asList(names));
            return this;
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
        public @Nonnull Builder withPagingStrategy(@Nonnull PagingStrategy pagingStrategy) {
            this.pagingStrategy = pagingStrategy;
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
                                           parser, pagingStrategy);
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
                String uri = getUri(pager.apply(input));
                Client client = createClient();
                parser.setupClient(client);
                WebTarget target = client.target(uri);
                Response response = target.request(parser.getAcceptable()).get();
                pager.notifyResponse(response);
                Object obj = response.readEntity(parser.getDesiredClass());
                CQEndpoint endpoint = parser.parse(obj, uri);
                pager.notifyResponseEndpoint(endpoint);
                return endpoint;
            }
        };
    }

    @Override
    public String toString() {
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
            bindings.put(name, serializer.toString(term, name, this));
        });
        if (!bindings.keySet().containsAll(getRequiredInputs())) {
            Set<String> missing = new HashSet<>(getRequiredInputs());
            missing.removeAll(bindings.keySet());
            throw new MissingAPIInputsException(missing, this);
        }
        return template.createURI(bindings);
    }
}
