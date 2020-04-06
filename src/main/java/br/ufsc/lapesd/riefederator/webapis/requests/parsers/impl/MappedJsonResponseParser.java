package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.requests.HTTPRequestInfo;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.ResponseParser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.jena.rdf.model.*;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.jena.rdf.model.ResourceFactory.createPlainLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;

@Immutable
public class MappedJsonResponseParser implements ResponseParser {
    private final @Nonnull ImmutableMap<String, String> context;
    private final @Nullable String prefixForNotMapped;
    @SuppressWarnings("Immutable")
    private final @Nullable PrimitiveParsersRegistry primitiveParsers;

    private static final String[] SELF_NAMES = {"@id", "_self", "_id", "self", "id"};
    private static final ImmutableSet<String> TRANSPARENT = ImmutableSet.of("_embedded");
    private static final ImmutableSet<String> BLACKLIST
            = ImmutableSet.of("_embedded", "_links", "links", "@id");
    private static final Pattern RX_ABS = Pattern.compile("^[^:]+://");
    private static final Pattern RX_SL_END = Pattern.compile("/+$");
    private static final Pattern RX_SL_BEGIN = Pattern.compile("^/+");


    public MappedJsonResponseParser(@Nonnull Map<String, String> context) {
        this(context, null);
    }

    public MappedJsonResponseParser(@Nonnull Map<String, String> context,
                                @Nullable String prefixForNotMapped) {
        this(context, prefixForNotMapped, null);
    }

    public MappedJsonResponseParser(@Nonnull Map<String, String> context,
                                    @Nullable String prefixForNotMapped,
                                    @Nullable PrimitiveParsersRegistry primitiveParsers) {
        this.context = ImmutableMap.copyOf(context);
        this.prefixForNotMapped = prefixForNotMapped;
        this.primitiveParsers = primitiveParsers;
    }

    public @Nullable PrimitiveParsersRegistry getPrimitiveParsers() {
        return primitiveParsers;
    }

    @Override
    public @Nonnull String[] getAcceptable() {
        return new String[] {APPLICATION_JSON, "application/hal+json"};
    }

    @Override
    public @Nonnull Class<?> getDesiredClass() {
        return String.class;
    }

    @Override
    public void setupClient(@Nonnull Client client) {
        /* no setup required */
    }

    @Override
    public @Nullable CQEndpoint parse(@Nullable Object object, @Nonnull String uriHint,
                                      @Nullable HTTPRequestInfo info) {
        if (object == null)
            return null;
        String string = (String) object;
        Model model = parseAsModel(string, info, uriHint);
        if (info != null)
            info.setParsedTriples((int) model.size());
        if (model.isEmpty())
            return null; // no data. Don't bother querying. If paging, stop
        return ARQEndpoint.forModel(model);
    }

    @NotNull
    public Model parseAsModel(@Nonnull String json, @Nullable HTTPRequestInfo info,
                              @Nonnull String uriHint) {
        Model model = ModelFactory.createDefaultModel();
        // parse json into model
        JsonElement element = new JsonParser().parse(json);
        if (info != null) {
            if (element.isJsonArray())
                info.setJsonRootArrayMembers(element.getAsJsonArray().size());
            if (element.isJsonObject())
                info.setJsonRootObjectMembers(element.getAsJsonObject().size());
        }
        parseInto(model, element, new ArrayList<>(), uriHint);
        return model;
    }

    protected RDFNode parseInto(@Nonnull Model model, @Nonnull JsonElement element,
                                @Nonnull List<String> path,
                                @Nullable String subjectHint) {
        if (element.isJsonArray()) {
            for (JsonElement e : element.getAsJsonArray())
                parseInto(model, e, path, null);
        } else if (element.isJsonObject()) {
            JsonObject jsonObj = element.getAsJsonObject();
            Resource subj =  createResource(model, jsonObj, subjectHint);
            for (String name : TRANSPARENT) {
                JsonElement value = jsonObj.get(name);
                if (value != null) addProperties(model, value, subj, path);
            }
            addProperties(model, jsonObj, subj, path); //only handles non-transparent
            return subj;
        } else if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
            String primitiveString = primitive.getAsString();
            if (primitiveParsers != null) {
                PrimitiveParser parser = primitiveParsers.get(path);
                if (parser != null)
                    return parser.parse(primitiveString);
            }
            if (primitive.isBoolean()) {
                return createTypedLiteral(primitive.getAsBoolean());
            } else if (primitive.isNumber()) {
                double value = primitive.getAsDouble();
                if (Math.floor(value) == value) {
                    if (value < Integer.MAX_VALUE && value > Integer.MIN_VALUE)
                        return createTypedLiteral((int)value);
                    return createTypedLiteral((long)value);
                }
                return createTypedLiteral(value);
            } else if (primitive.isString()) {
                Resource resource = tryParseURI(primitiveString);
                return resource == null ? createPlainLiteral(primitiveString) : resource;
            }
        }
        return null;
    }

    private @Nullable Resource tryParseURI(@Nonnull String string) {
        if (!RX_ABS.matcher(string).find())
            return null;
        try {
            return ResourceFactory.createResource(string);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void addProperties(@Nonnull Model model, @Nonnull JsonElement element,
                               @Nonnull Resource subj, @Nonnull List<String> path) {
        if (!element.isJsonObject()) return;
        for (Map.Entry<String, JsonElement> e : element.getAsJsonObject().entrySet()) {
            if (BLACKLIST.contains(e.getKey()))
                continue; // there are handled elsewhere
            Property prop = getProperty(model, e.getKey());
            if (prop == null) continue;
            path.add(e.getKey());
            RDFNode obj = parseInto(model, e.getValue(), path, null);
            path.remove(path.size()-1);
            if (obj != null)
                model.add(subj, prop, obj);
        }
    }

    private @Nullable Property getProperty(@Nonnull Model model, @Nonnull String jsonProperty) {
        String uri = context.get(jsonProperty);
        if (uri == null && prefixForNotMapped != null)
            uri = prefixForNotMapped + jsonProperty;
        return uri != null ? model.createProperty(uri) : null;
    }

    private @Nonnull Resource createResource(@Nonnull Model model, @Nonnull JsonObject o,
                                             @Nullable String uriHint) {
        String uri = makeAbsolute(tryGetURI(o, uriHint), uriHint);
        return uri == null ? model.createResource() : model.createResource(uri);
    }

    private @Nullable String makeAbsolute(@Nullable String uri, @Nullable String parent) {
        if (uri == null) return null;
        if (parent == null) return uri;
        if (RX_ABS.matcher(uri).find()) return uri;
        uri = RX_SL_BEGIN.matcher(uri).replaceFirst("");
        parent = RX_SL_END.matcher(parent).replaceFirst("");
        return parent + "/" + uri;
    }

    private String tryGetString(@Nullable JsonElement e, @Nonnull String propertyName) {
        if (e == null)
            return null;
        if (e.isJsonArray() && e.getAsJsonArray().size() > 0)
            e = e.getAsJsonArray().get(0);
        if (e.isJsonPrimitive())
            return e.getAsString();
        if (e.isJsonObject()) {
            JsonElement element = e.getAsJsonObject().get(propertyName);
            if (element == null)
                return null;
            if (element.isJsonArray() && element.getAsJsonArray().size() > 0)
                element = element.getAsJsonArray().get(0);
            if (element.isJsonPrimitive())
                return element.getAsString();
        }
        return null;
    }

    private boolean hasString(@Nullable JsonElement element, @Nonnull String property,
                              @Nonnull String expected) {
        String string = tryGetString(element, property);
        return string != null && string.trim().toLowerCase().equals(expected.trim().toLowerCase());
    }

    private @Nullable String tryGetSelfHref(@Nonnull JsonObject linksObj) {
        // Try self as a URI ("self": "...") or as an object with href ("self": {"href": "..."})
        JsonElement self = linksObj.getAsJsonObject().get("self");
        String uri;
        if ((uri = tryGetString(self, "href")) != null) return uri;
        if ((uri = tryGetString(self, "_href")) != null) return uri;

        // Try "links": {"rel": "self", "href": "..."} and variations
        if (hasString(linksObj, "rel", "self") || hasString(linksObj, "_rel", "self")) {
            if ((uri = tryGetString(linksObj, "href")) != null) return uri;
            if ((uri = tryGetString(linksObj, "_href")) != null) return uri;
        }
        return null;
    }

    private @Nullable String tryGetSelfHref(@Nonnull JsonObject o, @Nonnull String links) {
        JsonElement linksObj = o.get(links);
        if (linksObj == null) return null;
        if (linksObj.isJsonArray()) {
            for (JsonElement member : linksObj.getAsJsonArray()) {
                if (member.isJsonObject()) {
                    String string = tryGetSelfHref(member.getAsJsonObject());
                    if (string != null)
                        return string;
                }
            }
        } else if (linksObj.isJsonObject()) {
            return tryGetSelfHref(linksObj.getAsJsonObject());
        }
        return null;
    }

    @Contract("_, !null -> !null")
    private String tryGetURI(@Nonnull JsonObject o, @Nullable String fallback) {
        String uri;
        if ((uri = tryGetSelfHref(o, "_links")) != null) return uri;
        if ((uri = tryGetSelfHref(o, "links")) != null) return uri;
        for (String name : SELF_NAMES) {
            if ((uri = tryGetString(o, name)) != null) return uri;
        }
        return fallback;
    }

}
