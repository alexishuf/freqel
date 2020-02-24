package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.util.DictTree;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.ParamPagingStrategy;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.ResponseParser;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.TermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.OnlyNumbersTermSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl.SimpleDateSerializer;
import br.ufsc.lapesd.riefederator.webapis.requests.rate.RateLimitsRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import org.glassfish.jersey.uri.UriTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

public class SwaggerParser implements APIDescriptionParser {
    private static final Logger logger = LoggerFactory.getLogger(SwaggerParser.class);
    private static final List<String> reqRootProps = asList("swagger", "paths");

    private @Nonnull
    DictTree swagger;
    private @Nonnull ImmutableList<String> endpoints;
    private @Nonnull APIDescriptionContext fallbackContext = new APIDescriptionContext();

    public SwaggerParser(@Nonnull DictTree swagger) {
        this.swagger = swagger;
        this.endpoints = findEndpoints();
        String missing = reqRootProps.stream().filter(p -> !swagger.containsKey(p)).collect(joining(", "));
        if (!missing.isEmpty()) {
            throw new APIDescriptionParseException("Missing required properties: "+missing)
                    .setMap(this.swagger);
        }
    }

    /* --- --- --- --- Interface implementation --- --- --- --- */

    @Override
    public @Nonnull Collection<String> getEndpoints() {
        return endpoints;
    }

    @Override
    public @Nonnull APIDescriptionContext getFallbackContext() {
        return fallbackContext;
    }

    @Override
    public @Nonnull Molecule getMolecule(@Nonnull String endpoint,
                                         @Nullable APIDescriptionContext ctx) {
        JsonSchemaMoleculeParser parser = parseSchema(endpoint);
        return parser.getMolecule();
    }

    class Parameters {
        @Nonnull JsonSchemaMoleculeParser parser;
        @Nullable PagingStrategy pagingStrategy;
        @Nonnull Set<String> required, optional;
        @Nonnull Map<String, DictTree> paramObjMap;
        @Nonnull Map<String, ParameterPath> parameterPathMap;

        public Parameters(@Nonnull String endpoint, @Nullable APIDescriptionContext ctx,
                          @Nonnull JsonSchemaMoleculeParser parser) {
            this.parser = parser;
            this.pagingStrategy = getPagingStrategy(endpoint, ctx);
            this.paramObjMap = new HashMap<>();
            for (DictTree obj : getParams(endpoint))
                paramObjMap.put(obj.getPrimitive("name", "").toString(), obj);

            required = getParams(endpoint, true, emptySet(), emptySet());
            optional = getParams(endpoint, false, required, emptySet());
            if (pagingStrategy != null) {
                required.removeAll(pagingStrategy.getParametersUsed());
                optional.removeAll(pagingStrategy.getParametersUsed());
            }

            parameterPathMap = getParameterPaths(endpoint, parser, optional,
                    pagingStrategy == null ? emptySet() : pagingStrategy.getParametersUsed());
            Set<String> mappedInputs = new HashSet<>(parameterPathMap.keySet());
            if (pagingStrategy != null)
                mappedInputs.addAll(pagingStrategy.getParametersUsed());
            if (!mappedInputs.containsAll(required))
                throw ex("Some required inputs of endpoint %s have no atom mapped", endpoint);
        }

        @Nonnull DictTree getParamObj(@Nonnull String name) {
            if (!paramObjMap.containsKey(name)) throw new NoSuchElementException(name);
            return paramObjMap.get(name);
        }

        @Nonnull ParameterPath getParamPath(@Nonnull String name) {
            if (!parameterPathMap.containsKey(name)) throw new NoSuchElementException(name);
            return parameterPathMap.get(name);
        }

        @Nonnull Map<String, String> getAtom2Input() {
            Map<String, String> atom2Input = new HashMap<>(parameterPathMap.size());
            for (Map.Entry<String, ParameterPath> e : parameterPathMap.entrySet())
                atom2Input.put(e.getValue().getAtom().getName(), e.getKey());
            return atom2Input;
        }
    }

    @VisibleForTesting
    @Nonnull Parameters getParameters(@Nonnull String endpoint,
                                      @Nullable APIDescriptionContext ctx) {
        return new Parameters(endpoint, ctx, parseSchema(endpoint));
    }

    @Override
    public @Nonnull APIMolecule getAPIMolecule(@Nonnull String endpoint,
                                               @Nullable APIDescriptionContext ctx) {
        Parameters p = getParameters(endpoint, ctx);
        ResponseParser responseParser = getResponseParser(endpoint, ctx);
        if (responseParser == null)
            throw new NoSuchElementException("Could not get ResponseParser for endpoint "+endpoint);

        UriTemplateExecutor.Builder builder = UriTemplateExecutor.from(getTemplate(endpoint))
                .withOptional(p.optional).withRequired(p.required)
                .withResponseParser(responseParser);

        // apply modifiers to executor
        if (p.pagingStrategy != null)
            builder.withPagingStrategy(p.pagingStrategy);
        RateLimitsRegistry rateLimit = getRateLimitRegistry(endpoint, ctx);
        if (rateLimit != null)
            builder.withRateLimitsRegistry(rateLimit);

        // apply term serializers
        for (Map.Entry<String, DictTree> e : p.paramObjMap.entrySet()) {
            TermSerializer serializer = getTermSerializer(endpoint, e.getValue(), ctx);
            if (serializer != null)
                builder.withSerializer(e.getKey(), serializer);
        }

        return new APIMolecule(p.parser.getMolecule(), builder.build(), p.getAtom2Input(),
                               getCardinality(endpoint, p.parser.getCardinality()));
    }



    @Override
    public @Nonnull WebAPICQEndpoint getEndpoint(@Nonnull String endpoint,
                                                 @Nullable APIDescriptionContext ctx) {
        return new WebAPICQEndpoint(getAPIMolecule(endpoint, ctx));
    }

    /* --- --- --- --- private helper methods --- --- --- --- */

    @FormatMethod
    private @Nonnull APIDescriptionParseException ex(@Nonnull String format, Object... values) {
        String msg = String.format(format, values);
        return new APIDescriptionParseException(msg).setMap(swagger);
    }

    private @Nullable PagingStrategy parsePagingStrategy(@Nonnull String endpoint) {
        DictTree map = getPathObj(endpoint).getMapNN("get/x-paging");
        boolean fromRoot = map.isEmpty();
        if (fromRoot)
            map = swagger.getMapNN("x-paging");
        if (map.isEmpty())
            return null; // no x-paging

        List<DictTree> params = getParams(endpoint);
        Object paramName = map.get("param");
        if (paramName == null) {
            logger.warn("x-paging for endpoint {} misses required property \"param\"", endpoint);
            return null;
        }
        if (params.stream().noneMatch(m -> m.contains("name", paramName.toString()))) {
            if (!fromRoot) { //this is OK if x-paging was taking from the root
                logger.warn("x-paging for endpoint {} uses missing parameter {}",
                            endpoint, paramName);
            }
            return null;
        }

        long start = map.getLong("start", 1);
        long increment = map.getLong("increment", 1);
        ParamPagingStrategy.Builder builder = ParamPagingStrategy.builder(paramName.toString())
                .withFirstPage((int) start)
                .withIncrement((int) increment)
                .withEndOnNull(true);
        DictTree ev = map.getMapNN("endValue");
        if (!ev.isEmpty()) {
            String path = ev.getPrimitive("path", "").toString();
            if (path.isEmpty())
                logger.warn("Ignoring x-paging/endValue with no path for endpoint {}", endpoint);
            else
                builder.withEndOnJsonValue(path, ev.get("value"));
        }

        return builder.build();
    }

    @VisibleForTesting
    @Nullable PagingStrategy getPagingStrategy(@Nonnull String endpoint,
                                                       @Nullable APIDescriptionContext context) {
        PagingStrategy strategy = context != null ? context.getPagingStrategy(endpoint) : null;
        if (strategy == null) {
            strategy = parsePagingStrategy(endpoint);
            if (strategy == null)
                strategy = fallbackContext.getPagingStrategy(endpoint);
        }
        return strategy;
    }

    private @Nullable ResponseParser getResponseParser(@Nonnull String endpoint,
                                                       @Nullable APIDescriptionContext context) {
        ResponseParser parser = null;
        List<Object> mediaTypes;
        mediaTypes = new ArrayList<>(getPathObj(endpoint).getListNN("get/produces"));
        mediaTypes.add("application/json"); //fallback media type
        for (Object o : mediaTypes) {
            if (!(o instanceof String)) continue;
            String mediaType = o.toString();
            if (context != null)
                parser = context.getResponseParser(endpoint, mediaType);
            if (parser == null)
                parser = fallbackContext.getResponseParser(endpoint, mediaType);
            if (parser != null)
                return parser;
        }
        return null;
    }

    private @Nullable RateLimitsRegistry
    getRateLimitRegistry(@Nonnull String endpoint, @Nullable APIDescriptionContext context) {
        RateLimitsRegistry registry = null;
        if (context != null)
            registry = context.getRateLimitRegistry(endpoint);
        if (registry == null)
            registry = fallbackContext.getRateLimitRegistry(endpoint);
        return registry;
    }

    private @Nonnull List<DictTree> getParams(@Nonnull String endpoint) {
        List<Object> params = getPathObj(endpoint).getListNN("parameters");
        List<Object> opParams = getPathObj(endpoint).getListNN("get/parameters");
        List<Object> list = DictTree.override(params, opParams, "name");
        List<DictTree> casted = new ArrayList<>(list.size());
        for (Object o : list) {
            if (!(o instanceof DictTree))
                logger.info("Ignoring non-object param {} of endpoint {}", o, endpoint);
            else
                casted.add((DictTree) o);
        }
        return casted;
    }

    private @Nonnull Map<String, ParameterPath>
    getParameterPaths(@Nonnull String endpoint, @Nonnull JsonSchemaMoleculeParser parser,
                      @Nonnull Set<String> optional, @Nonnull Collection<String> pagingInputs) {
        Molecule molecule = parser.getMolecule();
        Map<String, ParameterPath> result = new HashMap<>();
        for (DictTree paramObj : getParams(endpoint)) {
            String name = paramObj.getPrimitive("name", "").toString();
            if (pagingInputs.contains(name))
                continue;
            DictTree xPath = paramObj.getMapNN("x-path");
            if (xPath.isEmpty() && optional.contains(name)) {
                logger.info("Ignoring optional input {} of endpoint {} as it cannot " +
                            "be mapped to an Atom", name, endpoint);
                optional.remove(name);
                continue;
            }
            StringBuilder b = new StringBuilder();
            ParameterPath pp = ParameterPath.tryParse(xPath, molecule, parser::prop2Term, b);
            if (pp == null) {
                logger.warn("Could not parse x-path of {} parameter {} on endpoint {}. Will " +
                            "ignore the parameter. Cause: {}",
                            optional.contains(name) ? "optional" : "required",
                            name, endpoint, b.toString());
                optional.remove(name);
            } else {
                result.put(name, pp);
            }
        }
        return result;
    }

    @VisibleForTesting
    @Nonnull Cardinality getCardinality(@Nonnull String endpoint, @Nonnull Cardinality fallback) {
        String string = getPathObj(endpoint)
                .getPrimitive("get/responses/200/x-cardinality", "").toString();
        Cardinality cardinality = Cardinality.parse(string);
        return cardinality != null ? cardinality : fallback;
    }

    private @Nullable SimpleDateSerializer parseDateSerializer(@Nonnull DictTree paramObj) {
        if (!paramObj.contains("serializer", "date")) return null;
        String dateFmt = paramObj.getPrimitive("date-format", "").toString();
        if (!dateFmt.isEmpty()) {
            if (!SimpleDateSerializer.isValidFormat(dateFmt)) {
                logger.warn("The given date format {} is not valid. " +
                        "See SimpleDateFormat javadoc", dateFmt);
                return null;
            }
            return new SimpleDateSerializer(dateFmt);
        }
        return null;
    }

    private @Nullable OnlyNumbersTermSerializer
    parseOnlyNumbersSerializer(@Nonnull DictTree paramObj) {
        if (!paramObj.contains("serializer", "only-numbers"))
            return null;
        long width = paramObj.getLong("width", Long.MIN_VALUE);
        if (width != Long.MIN_VALUE && width < 1) {
            logger.warn("Ignoring invalid width {} for OnlyNumbersTermSerializer", width);
            width = Long.MIN_VALUE;
        }
        String fill = paramObj.getPrimitive("fill", "0").toString();
        if (fill.length() != 1) {
            fill = fill.trim();
            if (fill.length() != 1) {
                logger.warn("Ignoring fill=\"{}\" on only-numbers serializer (bad length)", fill);
                fill = "0";
            }
        }
        OnlyNumbersTermSerializer.Builder builder = OnlyNumbersTermSerializer.builder();
        builder.setFill(fill.charAt(0));
        if (width != Long.MIN_VALUE)
            builder.setWidth((int) width);
        return builder.build();
    }

    private @Nullable TermSerializer parseTermSerializer(@Nonnull DictTree paramObj) {
        DictTree map = paramObj.getMapNN("x-serializer");
        if (map.isEmpty())
            return null; //nothing to parse
        TermSerializer serializer = parseDateSerializer(paramObj);
        if (serializer != null)
            return serializer;
        serializer = parseOnlyNumbersSerializer(paramObj);
        return serializer;
    }

    private @Nullable TermSerializer getTermSerializer(@Nonnull String endpoint,
                                                       @Nonnull DictTree paramObj,
                                                       @Nullable APIDescriptionContext ctx) {
        String name = paramObj.getPrimitive("name", "").toString();
        TermSerializer serializer = ctx != null ? ctx.getSerializer(endpoint, name) : null;
        if (serializer == null) {
            serializer = parseTermSerializer(paramObj);
            if (serializer == null)
                serializer = fallbackContext.getSerializer(endpoint, name);
        }
        return serializer;
    }

    private @Nonnull Set<String> getParams(@Nonnull String endpoint, boolean isRequired,
                                            @Nonnull Set<String> required,
                                            @Nonnull Set<String> optional) {
        Set<String> set = new HashSet<>();
        for (Object param : getParams(endpoint)) {
            if (!(param instanceof DictTree)) {
                logger.info("Ignoring non-object parameter param {} endpoint {}", param, endpoint);
                continue;
            }
            DictTree paramMap = (DictTree) param;
            Object name = paramMap.get("name");
            if (name == null) {
                logger.info("Ignoring unnamed parameter in path {}, \"get\" operation", endpoint);
            } else {
                boolean include = (isRequired && required.contains(name.toString())) ||
                        (!isRequired && optional.contains(name.toString())) ||
                        (paramMap.contains("required", true) == isRequired);
                if (include)
                    set.add(name.toString());
            }
        }
        return set;
    }

    private @Nonnull String getScheme() {
        List<Object> schemes = swagger.getListNN("schemes");
        if (schemes.contains("https")) return "https"; //prefer if available
        return "http"; // fallback if schemes empty
    }

    private @Nonnull UriTemplate getTemplate(@Nonnull String endpoint) {
        String builder = String.valueOf(swagger.getOrDefault("host", "")) + '/' +
                swagger.getOrDefault("basePath", "") + '/' + endpoint;
        String proto = builder.replaceAll("//+", "/");
        if (!proto.matches("^https?://"))
            proto = getScheme() + proto;
        return new UriTemplate(proto);
    }

    private @Nonnull
    DictTree getPathObj(@Nonnull String endpoint) {
        String escaped = endpoint.replaceAll("/", "%2F");
        DictTree map = swagger.getMapNN("paths/" + escaped);
        if (map.isEmpty())
            throw ex("Path object not found for endpoint %s", endpoint);
        return map;
    }

    private @Nonnull JsonSchemaMoleculeParser parseSchema(@Nonnull String endpoint) {
        DictTree schema = getPathObj(endpoint).getMapNN("get/responses/200/schema");
        if (schema.isEmpty())
            throw ex("API path %s has no schema on operation", endpoint);
        JsonSchemaMoleculeParser parser = new JsonSchemaMoleculeParser();
        parser.parse(schema);
        return parser;
    }

    private @Nonnull ImmutableList<String> findEndpoints() {
        DictTree paths = swagger.getMap("paths");
        if (paths == null) {
            logger.warn("No paths in Swagger!");
            return ImmutableList.of();
        }
        List<String> list = new ArrayList<>();
        for (String path : paths.keySet()) {
            DictTree op = getPathObj(path);
            if (op.isEmpty()) continue;
            if (op.getMapNN("get/responses/200/schema").isEmpty()) continue;

            list.add(path);
        }
        return ImmutableList.copyOf(list);
    }

    /* --- --- --- --- --- --- Factory --- --- --- --- --- --- */

    public static class Factory extends AbstractAPIDescriptionParserFactory {
        public static  @Nonnull SwaggerParser fromDict(@Nonnull DictTree root) {
            Queue<DictTree> queue = new ArrayDeque<>();
            queue.add(root);
            while (!queue.isEmpty()) {
                DictTree obj = queue.remove();
                if (obj.containsKey("swagger"))
                    return new SwaggerParser(obj.asRoot());
                if (obj.containsKey("baseSwagger")) {
                    List<List<String>> idPaths = singletonList(asList("parameters", "name"));
                    DictTree base = obj.getMapNN("baseSwagger");
                    DictTree overlay = obj.getMapNN("overlay");
                    return new SwaggerParser(DictTree.overlay(base, overlay, idPaths).asRoot());
                }
                for (String key : obj.keySet()) {
                    DictTree map = obj.getMap(key);
                    if (map != null) queue.add(map);
                }
            }
            throw new APIDescriptionParseException("No swagger property found in "+root);
        }

        @Override
        public @Nonnull APIDescriptionParser
        fromInputStream(@Nonnull InputStream inputStream) throws IOException {
            return fromDict(DictTree.load().fromInputStream(inputStream));
        }

        @Override
        public @Nonnull SwaggerParser fromFile(@Nonnull File file) throws IOException {
            return fromDict(DictTree.load().fromFile(file));
        }

        @Override
        public @Nonnull SwaggerParser fromURL(@Nonnull String url) throws IOException {
            return (SwaggerParser) super.fromURL(url);
        }

        @Override
        public @Nonnull SwaggerParser fromResource(@Nonnull String resourcePath) throws IOException{
            return fromDict(DictTree.load(resourcePath).fromResource(resourcePath));
        }

        @Override
        public @Nonnull SwaggerParser fromResource(@Nonnull Class<?> cls,
                                                   @Nonnull String resourcePath) throws IOException{
            return fromDict(DictTree.load(resourcePath).fromResource(cls, resourcePath));
        }
    }

    public static final @Nonnull Factory FACTORY = new Factory();
    public static @Nonnull Factory getFactory() {
        return FACTORY;
    }
    @Override
    public @Nonnull APIDescriptionParserFactory createFactory() {
        return FACTORY;
    }
}
