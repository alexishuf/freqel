package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.requests.ResponseParser;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.Model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;

@Immutable
public class JenaResponseParser implements ResponseParser {
    public static final @Nonnull JenaResponseParser INSTANCE = new JenaResponseParser();

    @Override
    public @Nonnull String[] getAcceptable() {
        return ModelMessageBodyReader.getSupportedMediaTypes();
    }

    @Override
    public @Nonnull Class<?> getDesiredClass() {
        return Model.class;
    }

    @Override
    public void setupClient(@Nonnull Client client) {
        client.register(ModelMessageBodyReader.class);
    }

    @Override
    public @Nonnull CQEndpoint parse(@Nullable Object object, @Nonnull String uriHint) {
        if (object == null) return new EmptyEndpoint();
        return ARQEndpoint.forModel((Model) object);
    }
}
