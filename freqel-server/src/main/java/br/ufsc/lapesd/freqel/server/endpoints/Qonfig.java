package br.ufsc.lapesd.freqel.server.endpoints;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Path("qonfig.js")
public class Qonfig {

    @GET
    @Produces("text/javascript")
    public @Nonnull String get(@Context UriInfo uriInfo) {
        String host = uriInfo.getRequestUri().getHost();
        int port = uriInfo.getRequestUri().getPort();
        if (port > 0 && port != 80)
            host += ":"+port;
        try (InputStream in = getClass().getResourceAsStream("qonfig.js.tpl")) {
            if (in == null) {
                String msg = "Could not find resource qonfig.js.tpl";
                throw new InternalServerErrorException(msg,
                        Response.serverError().type(MediaType.TEXT_PLAIN_TYPE).entity(msg).build());
            }
            String string = IOUtils.toString(in, StandardCharsets.UTF_8);
            return string.replaceAll("\\$HOST", host);
        } catch (IOException e) {
            String msg = "Failed to read qonfig.js.tpl as a resource: " + e.getMessage();
            throw new InternalServerErrorException(msg, Response.serverError()
                    .type(MediaType.TEXT_PLAIN_TYPE).entity(msg).build(), e);
        }
    }
}
