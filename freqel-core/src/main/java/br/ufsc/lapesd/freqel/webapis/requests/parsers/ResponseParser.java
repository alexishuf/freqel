package br.ufsc.lapesd.freqel.webapis.requests.parsers;

import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.webapis.requests.HTTPRequestInfo;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;

@Immutable
public interface ResponseParser {
    @Nonnull String[] getAcceptable();
    @Nonnull Class<?> getDesiredClass();
    void setupClient(@Nonnull Client client);
    @Nullable CQEndpoint parse(@Nullable Object object, @Nonnull String uriHint, @Nullable HTTPRequestInfo info);
    default @Nullable CQEndpoint parse(@Nullable Object object, @Nonnull String uriHint) {
        return parse(object, uriHint, null);
    }
}
