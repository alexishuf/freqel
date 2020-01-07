package br.ufsc.lapesd.riefederator.webapis.requests;

import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;

@Immutable
public interface ResponseParser {
    @Nonnull String[] getAcceptable();
    @Nonnull Class<?> getDesiredClass();
    void setupClient(@Nonnull Client client);
    @Nonnull CQEndpoint parse(@Nullable Object object, @Nonnull String uriHint);
}
