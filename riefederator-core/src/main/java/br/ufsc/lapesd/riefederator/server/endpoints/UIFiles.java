package br.ufsc.lapesd.riefederator.server.endpoints;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Path("ui")
public class UIFiles {
    private static final  @Nonnull Map<String, MediaType> ext2mt;

    static {
        Map<String, MediaType> map = new HashMap<>();
        map.put("html", MediaType.TEXT_HTML_TYPE);
        map.put("htm", MediaType.TEXT_HTML_TYPE);
        map.put("txt", MediaType.TEXT_PLAIN_TYPE);
        map.put("js", new MediaType("text", "javascript"));
        map.put("css", new MediaType("text", "css"));
        map.put("csv", new MediaType("text", "csv"));
        map.put("tsv", new MediaType("text", "tsv"));
        map.put("json", MediaType.APPLICATION_JSON_TYPE);
        map.put("png", new MediaType("image", "png"));
        map.put("svg", MediaType.APPLICATION_SVG_XML_TYPE);
        map.put("jpg", new MediaType("image", "jpeg"));
        map.put("gif", new MediaType("image", "gif"));
        map.put("jpeg", new MediaType("image", "jpeg"));
        ext2mt = map;
    }

    @GET
    @Path("{path:.*}")
    public Response get(@PathParam("path") String path) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream stream = cl.getResourceAsStream("ui/" +path);
        if (stream == null)
            throw new NotFoundException();
        String ext = path.replaceAll("^.*\\.([^.]+)$", "$1");
        MediaType mediaType = ext2mt.getOrDefault(ext, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        return Response.ok(stream, mediaType).build();
    }
}
