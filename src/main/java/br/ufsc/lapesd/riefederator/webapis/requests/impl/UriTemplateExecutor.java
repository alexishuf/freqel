package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.MissingAPIInputsException;
import br.ufsc.lapesd.riefederator.webapis.requests.ResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.TermSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.uri.UriTemplate;

import javax.annotation.Nonnull;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.*;

@Immutable
public class UriTemplateExecutor implements APIRequestExecutor {
    protected final @SuppressWarnings("Immutable") @Nonnull UriTemplate template;
    protected final @Nonnull ImmutableSet<String> required, optional;
    private final @SuppressWarnings("Immutable") @Nonnull ImmutableMap<String, TermSerializer> input2serializer;
    protected final @SuppressWarnings("Immutable") @Nonnull ClientConfig clientConfig;
    private final @Nonnull ResponseParser parser;

    public UriTemplateExecutor(@Nonnull UriTemplate template, @Nonnull ImmutableSet<String> required,
                               @Nonnull ImmutableSet<String> optional,
                               @Nonnull Map<String, TermSerializer> input2serializer,
                               @Nonnull ClientConfig clientConfig,
                               @Nonnull ResponseParser parser) {
        this.template = template;
        this.required = required;
        this.optional = optional;
        this.input2serializer = ImmutableMap.copyOf(input2serializer);
        this.clientConfig = clientConfig;
        this.parser = parser;
    }

    public UriTemplateExecutor(@Nonnull UriTemplate template) {
        this(template, ImmutableSet.copyOf(template.getTemplateVariables()), ImmutableSet.of(),
             ImmutableMap.of(), new ClientConfig(), JenaResponseParser.INSTANCE);
    }

    public static class Builder {
        private final @Nonnull UriTemplate template;
        private final  @Nonnull ImmutableSet.Builder<String> required = ImmutableSet.builder();
        private final  @Nonnull ImmutableSet.Builder<String> optional = ImmutableSet.builder();
        private final @Nonnull ImmutableMap.Builder<String, TermSerializer> input2serializer
                = ImmutableMap.builder();
        private @Nonnull ClientConfig clientConfig = new ClientConfig();
        private @Nonnull ResponseParser parser = JenaResponseParser.INSTANCE;

        public Builder(@Nonnull UriTemplate template) {
            this.template = template;
        }

        public @Nonnull Builder withRequired(@Nonnull String... names) {
            required.add(names);
            return this;
        }
        public @Nonnull Builder withOptional(@Nonnull String... names) {
            optional.add(names);
            return this;
        }
        public @Nonnull Builder withClientConfig(@Nonnull ClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            return this;
        }
        public @Nonnull Builder withResponseParser(@Nonnull ResponseParser parser) {
            this.parser = parser;
            return this;
        }
        public @Nonnull Builder withSerializer(@Nonnull String input,
                                               @Nonnull TermSerializer serializer) {
            input2serializer.put(input, serializer);
            return this;
        }
        public @Nonnull UriTemplateExecutor build() {
            return new UriTemplateExecutor(template, required.build(), optional.build(),
                                           input2serializer.build(), clientConfig, parser);
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
        String uri = getUri(input);
        Client client = createClient();
        parser.setupClient(client);
        WebTarget target = client.target(uri);
        Object obj = target.request(parser.getAcceptable()).get(parser.getDesiredClass());
        CQEndpoint endpoint = parser.parse(obj, uri);
        return Collections.singleton(endpoint).iterator();
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
