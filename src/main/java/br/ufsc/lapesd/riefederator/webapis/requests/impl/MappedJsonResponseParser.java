package br.ufsc.lapesd.riefederator.webapis.requests.impl;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.requests.ResponseParser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.jena.rdf.model.*;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import java.util.Map;
import java.util.regex.Pattern;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.jena.rdf.model.ResourceFactory.createPlainLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;

@Immutable
public class MappedJsonResponseParser implements ResponseParser {
    private final @Nonnull ImmutableMap<String, String> context;
    private final @Nullable String prefixForNotMapped;

    private static final String[] SELF_NAMES = {"@id", "_self", "_id", "self", "id"};
    private static final ImmutableSet<String> TRANSPARENT = ImmutableSet.of("_embedded");
    private static final ImmutableSet<String> BLACKLIST
            = ImmutableSet.of("_embedded", "_links", "@id");
    private static final Pattern RX_ABS = Pattern.compile("^[^:]+://");
    private static final Pattern RX_SL_END = Pattern.compile("/+$");
    private static final Pattern RX_SL_BEGIN = Pattern.compile("^/+");


    public MappedJsonResponseParser(@Nonnull Map<String, String> context) {
        this(context, null);
    }

    public MappedJsonResponseParser(@Nonnull Map<String, String> context,
                                    @Nullable String prefixForNotMapped) {
        this.context = ImmutableMap.copyOf(context);
        this.prefixForNotMapped = prefixForNotMapped;
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
    public @Nonnull CQEndpoint parse(Object object, @Nonnull String uriHint) {
        String string = (String) object;
        Model model = ModelFactory.createDefaultModel();
        // parse json into model
        JsonElement element = new JsonParser().parse(string);
        parseInto(model, element, uriHint);
        return ARQEndpoint.forModel(model);
    }

    private RDFNode parseInto(@Nonnull Model model, @Nonnull JsonElement element,
                              @Nullable String subjectHint) {
        if (element.isJsonArray()) {
            for (JsonElement e : element.getAsJsonArray())
                parseInto(model, e, null);
        } else if (element.isJsonObject()) {
            JsonObject jsonObj = element.getAsJsonObject();
            Resource subj =  createResource(model, jsonObj, subjectHint);
            for (String name : TRANSPARENT) {
                JsonElement value = jsonObj.get(name);
                if (value != null) addProperties(model, value, subj);
            }
            addProperties(model, jsonObj, subj); //only handles non-transparent
            return subj;
        } else if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
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
                Resource resource = tryParseURI(primitive.getAsString());
                return resource == null ? createPlainLiteral(primitive.getAsString()) : resource;
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
                               @Nonnull Resource subj) {
        if (!element.isJsonObject()) return;
        for (Map.Entry<String, JsonElement> e : element.getAsJsonObject().entrySet()) {
            if (BLACKLIST.contains(e.getKey()))
                continue; // there are handled elsewhere
            Property prop = getProperty(model, e.getKey());
            if (prop == null) continue;
            RDFNode obj = parseInto(model, e.getValue(), null);
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

    private String tryGetString(@Nonnull JsonElement e, @Nonnull String propertyName) {
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

    @Contract("_, !null -> !null")
    private String tryGetURI(@Nonnull JsonObject o, @Nullable String fallback) {
        JsonObject links = o.getAsJsonObject("_links");
        if (links != null) {
            JsonElement self = links.get("self");
            String uri = tryGetString(self, "href");
            if (uri != null)
                return uri;
        }
        for (String name : SELF_NAMES) {
            String uri = tryGetString(o, name);
            if (uri != null)
                return uri;
        }
        return fallback;
    }

}
