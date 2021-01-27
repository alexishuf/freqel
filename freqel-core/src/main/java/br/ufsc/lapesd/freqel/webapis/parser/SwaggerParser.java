package br.ufsc.lapesd.freqel.webapis.parser;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.description.molecules.AtomFilter;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.freqel.webapis.description.APIMolecule;
import br.ufsc.lapesd.freqel.webapis.description.IndexedParam;
import br.ufsc.lapesd.freqel.webapis.requests.impl.UriTemplateExecutor;
import br.ufsc.lapesd.freqel.webapis.requests.paging.PagingStrategy;
import br.ufsc.lapesd.freqel.webapis.requests.paging.impl.ParamPagingStrategy;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.PrimitiveParser;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.ResponseParser;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.TermSerializer;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.*;
import br.ufsc.lapesd.freqel.webapis.requests.rate.RateLimitsRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.FormatMethod;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.glassfish.jersey.uri.UriTemplate;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class SwaggerParser implements APIDescriptionParser {
    private static final Logger logger = LoggerFactory.getLogger(SwaggerParser.class);
    private static final List<String> reqRootProps = asList("swagger", "paths");

    private static final List<String> DO_NOT_NULLIFY =
            asList("paths", "definitions", "parameters", "responses");

    private @Nonnull final DictTree swagger;
    private @Nonnull final ImmutableSet<String> endpoints;
    private @Nonnull final APIDescriptionContext fallbackContext = new APIDescriptionContext();

    private static DictTree doOverlay(@Nonnull DictTree baseSwagger, @Nonnull DictTree overlay) {
        List<List<String>> idPaths = singletonList(asList("parameters", "name"));
        for (String key : DO_NOT_NULLIFY) {
            if (overlay.containsKey(key) && overlay.get(key) == null)
                logger.warn("Swagger root property {} is nullified by overlay {}", key, overlay);
        }
        return DictTree.overlay(baseSwagger, overlay, idPaths).asRoot();
    }

    public SwaggerParser(@Nonnull DictTree baseSwagger, @Nonnull DictTree overlay) {
        this(doOverlay(baseSwagger, overlay));
    }

    public SwaggerParser(@Nonnull DictTree swagger) {
        this.swagger = swagger;
        this.endpoints = findEndpoints();
        String missing = reqRootProps.stream().filter(p -> !swagger.containsKey(p)).collect(joining(", "));
        if (!missing.isEmpty()) {
            throw new APIDescriptionParseException("Missing required properties: "+missing)
                    .setMap(this.swagger);
        }
    }

    @CanIgnoreReturnValue
    public @Nullable String setHost(@Nonnull String host) {
        Object old = swagger.put("host", host);
        return old == null ? null : old.toString();
    }

    public @Nullable String getHost() {
        Object host = swagger.get("host");
        return host == null ? null : host.toString();
    }

    /* --- --- --- --- Interface implementation --- --- --- --- */

    @Override
    public @Nonnull ImmutableSet<String> getEndpoints() {
        return endpoints;
    }

    public @Nonnull ImmutableSet<ImmutablePair<String, String>> getEquivalences() {
        Set<ImmutablePair<String, String>> equivalences = new HashSet<>();
        for (String ep : getEndpoints()) {
            for (Object obj : getPathObj(ep).getListNN("get/x-equiv-paths")) {
                if (obj instanceof String) {
                    String o = obj.toString();
                    int d = ep.compareTo(o);
                    if (d == 0) continue;
                    equivalences.add(ImmutablePair.of(d < 0 ? ep : o, d < 0 ? o : ep));
                } else {
                    logger.warn("Ignoring non-string swagger path: {}", obj);
                }
            }
        }
        return ImmutableSet.copyOf(equivalences);
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
        final @Nonnull JsonSchemaMoleculeParser parser;
        final @Nonnull String endpointKey;
        final @Nullable PagingStrategy pagingStrategy;
        @Nonnull Set<String> required, optional, missing;
        @Nonnull Set<String> execRequired, execOptional;
        @Nonnull Map<String, String> naValues;
        final @Nonnull Map<String, DictTree> paramObjMap;
        final @Nonnull Map<String, ParameterPath> parameterPathMap;
        @Nonnull Map<String, String> listSeparators;

        public Parameters(@Nonnull String endpoint, @Nullable APIDescriptionContext ctx,
                          @Nonnull JsonSchemaMoleculeParser parser) {
            this.endpointKey = endpoint;
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
            missing = parameterPathMap.entrySet().stream()
                    .filter(e -> e.getValue().isMissingInResult())
                    .map(Map.Entry::getKey).collect(toSet());
            Set<String> mappedInputs = new HashSet<>(parameterPathMap.keySet());
            if (pagingStrategy != null)
                mappedInputs.addAll(pagingStrategy.getParametersUsed());
            if (!mappedInputs.containsAll(required))
                throw ex("Some required inputs of endpoint %s have no atom mapped", endpoint);
            execRequired = new HashSet<>();
            execOptional = new HashSet<>();
            expandIndexed(endpoint, required, true, execRequired, execOptional);
            expandIndexed(endpoint, optional, false, execRequired, execOptional);
            naValues = new HashMap<>();
            addNaValues(naValues, execRequired);
            addNaValues(naValues, execOptional);

            listSeparators = new HashMap<>();
            for (Map.Entry<String, DictTree> e : paramObjMap.entrySet()) {
                if ("array".equals(e.getValue().getString("type"))) {
                    DictTree items = e.getValue().getMapNN("items");
                    String fmt = items.getString("collectionFormat", "csv").trim().toLowerCase();
                    String separator;
                    switch (fmt) {
                        case "csv":   separator = "," ; break;
                        case "ssv":   separator = " " ; break;
                        case "tsv":   separator = "\t"; break;
                        case "pipes": separator = "|" ; break;
                        case "multi": separator = null; break;
                        default:
                            throw ex("Unexpected collectionFormat=%s for parameter " +
                                     "%s in endpoint %s", fmt, e.getKey(), endpoint);
                    }
                    listSeparators.put(e.getKey(), separator);
                }
            }
        }

        private void addNaValues(Map<String, String> naValues, Set<String> expandedNames) {
            for (String expandedName : expandedNames) {
                IndexedParam indexed = IndexedParam.parse(expandedName);
                if (indexed != null) {
                    ParameterPath pp = parameterPathMap.get(indexed.base);
                    String naValue = pp == null ? "" : pp.getNaValue();
                    assert pp != null;
                    naValues.put(indexed.base, naValue);
                }
            }
        }

        private void expandIndexed(@Nonnull String endpoint, @Nonnull Set<String> paramNames,
                                   boolean paramRequired,
                                   @Nonnull Set<String> outReq, @Nonnull Set<String> outOpt) {
            for (String name : paramNames) {
                ParameterPath pp = parameterPathMap.get(name);
                if (pp.getAtomFilters().stream().anyMatch(AtomFilter::hasInputIndex)) {
                    //if no indexed filter is required, can bind directly to the param
                    if (pp.getAtomFilters().stream().noneMatch(AtomFilter::isRequired))
                        (paramRequired ? outReq : outOpt).add(name);
                    int size = pp.getAtomFilters().size();
                    for (AtomFilter f : pp.getAtomFilters()) {
                        if (!f.hasInputIndex())
                            throw ex("%f of endpoint %s missing index", f, endpoint);
                        Set<String> set = paramRequired && f.isRequired() ? outReq : outOpt;
                        set.add(IndexedParam.index(name, f.getInputIndex(), size));
                    }
                } else {
                    (paramRequired ? outReq : outOpt).add(name);
                }
            }
        }

        private boolean containsParam(@Nonnull String tpl, @Nonnull String name) {
            name = name.replaceAll("\\.", "\\.");
            Pattern pattern = Pattern.compile("\\{[?&]?([^}]+,)?" + name + "(,[^}]+)?}");
            return pattern.matcher(tpl).find();
        }

        private @Nonnull String addQueryParam(@Nonnull String tpl, @Nonnull String name) {
            Preconditions.checkState(!containsParam(tpl, name));
            String tpl2 = tpl.replaceAll("\\{([&?][^}]+)}(?!\\{[&?])", "{$1," + name + "}");
            if (tpl2.equals(tpl))
                tpl2 = tpl.replaceAll("(#|$)", "{?"+name+"}$1");
            assert containsParam(tpl2, name);
            return tpl2;
        }

        @Nonnull UriTemplate getTemplate() {
            String template = getTemplateBase(endpointKey);
            for (DictTree d : paramObjMap.values()) {
                String name = d.getPrimitive("name", "").toString();
                if (name.isEmpty())
                    continue; // no name!
                if (containsParam(template, name))
                    continue; // no work to do
                if (d.getPrimitive("in", "query").toString().equals("query"))
                    template = addQueryParam(template, name);
            }
            return new UriTemplate(template);
        }

        @Nonnull DictTree getParamObj(@Nonnull String name) {
            if (!paramObjMap.containsKey(name)) throw new NoSuchElementException(name);
            return paramObjMap.get(name);
        }

        @Nonnull ParameterPath getParamPath(@Nonnull String name) {
            if (!parameterPathMap.containsKey(name)) throw new NoSuchElementException(name);
            return parameterPathMap.get(name);
        }

        @Nonnull Collection<ParameterPath> getParamPaths() {
            assert new HashSet<>(parameterPathMap.values()).size() == parameterPathMap.size();
            return parameterPathMap.values();
        }

        @Nonnull Map<String, String> getElement2Input() {
            Map<String, String> element2Input = new HashMap<>(parameterPathMap.size());
            for (Map.Entry<String, ParameterPath> e : parameterPathMap.entrySet()) {
                Collection<AtomFilter> atomFilters = e.getValue().getAtomFilters();
                if (atomFilters.isEmpty()) {
                    element2Input.put(e.getValue().getAtom().getName(), e.getKey());
                } else {
                    // if no filter is required, can also bind directly to the parameter
                    if (atomFilters.stream().noneMatch(AtomFilter::isRequired))
                        element2Input.put(e.getValue().getAtom().getName(), e.getKey());
                    int size = atomFilters.size();
                    for (AtomFilter atomFilter : atomFilters) {
                        String input = e.getKey();
                        if (atomFilter.hasInputIndex())
                            input = IndexedParam.index(input, atomFilter.getInputIndex(), size);
                        element2Input.put(atomFilter.getName(), input);
                    }
                }
            }
            return element2Input;
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
        ResponseParser responseParser = getResponseParser(endpoint, p.parser, ctx);
        if (responseParser == null)
            throw new NoSuchElementException("Could not get ResponseParser for endpoint "+endpoint);

        UriTemplate template = p.getTemplate();
        UriTemplateExecutor.Builder builder = UriTemplateExecutor.from(template)
                .withOptional(p.execOptional).withRequired(p.execRequired)
                .withMissingInResult(p.missing)
                .withResponseParser(responseParser);
        p.naValues.forEach(builder::withIndexedNAValue);
        p.listSeparators.forEach(builder::withListSeparator);

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

        MoleculeBuilder moleculeBuilder = p.parser.getMolecule().toBuilder();
        p.getParamPaths().stream().flatMap(pp -> pp.getAtomFilters().stream())
                                  .forEach(moleculeBuilder::filter);
        return new APIMolecule(moleculeBuilder.build(), builder.build(), p.getElement2Input(),
                               getCardinality(endpoint, p.parser.getCardinality()),
                               template.getTemplate());
    }

    @Override
    public @Nonnull WebAPICQEndpoint getEndpoint(@Nonnull String endpoint,
                                                 @Nullable APIDescriptionContext ctx) {
        if (!endpoints.contains(endpoint)) {
            Pattern rx = Pattern.compile(endpoint.replaceAll("/*$", "/*").replaceAll("^/*", "/*"));
            String fix = endpoints.stream().filter(e -> rx.matcher(e).matches())
                                  .findFirst().orElse(null);
            if (fix == null)
                throw ex("Endpoint %s not found (ignoring start/end '/'s)", endpoint);
            endpoint = fix;
        }
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
            String path = ev.getString("path", "");
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

    private @Nonnull List<String> getEndpointProducedMediaTypes(@Nonnull String endpoint) {
        DictTree pathObj = getPathObj(endpoint);
        List<Object> mediaTypes = new ArrayList<>(pathObj.getListNN("get/produces"));
        mediaTypes.addAll(swagger.getListNN("produces"));
        mediaTypes.removeIf(mt ->
                !(mt instanceof String) || mt.toString().trim().startsWith("*/*"));
        if (mediaTypes.isEmpty())
            mediaTypes.add("application/json"); //fallback media type
        //noinspection unchecked
        return (List<String>)((List<?>)mediaTypes);
    }

    @VisibleForTesting
    @Nullable ResponseParser getResponseParser(@Nonnull String endpoint,
                                               @Nonnull JsonSchemaMoleculeParser schemaParser,
                                               @Nullable APIDescriptionContext context) {
        ResponseParser parser = null;
        List<String> mediaTypes = getEndpointProducedMediaTypes(endpoint);
        DictTree pathObj = getPathObj(endpoint);
        for (Object o : mediaTypes) {
            String mediaType = o.toString();
            if (context != null)
                parser = context.getResponseParser(endpoint, mediaType);
            if (parser == null)
                parser = fallbackContext.getResponseParser(endpoint, mediaType);
            if (parser != null)
                return parser;
        }
        ProtoResponseParser proto = findResponseParser(pathObj.getMapNN("get"), mediaTypes);
        if (proto == null)
            proto = findResponseParser(swagger, mediaTypes);
        if (proto == null && mediaTypes.contains("application/json")) {// fallback to urn:plain
            return new MappedJsonResponseParser(Collections.emptyMap(), StdPlain.URI_PREFIX,
                                                schemaParser.getParsersRegistry());
        }
        return proto != null ? proto.parse() : null;
    }

    private @Nullable ProtoResponseParser findResponseParser(@Nonnull DictTree specsParent,
                                                             @Nonnull List<?> mediaTypes) {
        for (ProtoResponseParser proto : getProtoResponseParsers(specsParent)) {
            for (Object mediaType : mediaTypes) {
                if (mediaType instanceof String && proto.accepts(mediaType.toString()))
                    return proto;
            }
        }
        return null;
    }

    private @Nonnull List<ProtoResponseParser> getProtoResponseParsers(@Nonnull DictTree parent) {
        List<ProtoResponseParser> list = new ArrayList<>();
        for (Object o : parent.getListNN("x-response-parser")) {
            if (!(o instanceof DictTree)) {
                logger.error("Expected a JSON object at {}/x-response-parser, got {}", parent, o);
                continue;
            }
            list.add(new ProtoResponseParser(((DictTree)o)));
        }
        return list;
    }

    private class ProtoResponseParser {
        private final DictTree spec;

        public ProtoResponseParser(@Nonnull DictTree spec) {
            this.spec = spec;
        }

        public boolean accepts(@Nonnull String mediaType) {
            MediaType accept = MediaType.valueOf(spec.getString("media-type", "*/*"));
            return accept.isCompatible(MediaType.valueOf(mediaType));
        }

        public @Nonnull String getName() {
            String name = spec.getString("response-parser");
            if (name == null)
                throw ex("Missing property response-parser for x-response-parser at %s", spec);
            return name;
        }

        public @Nonnull ResponseParser parse() {
            String name = spec.getString("response-parser");
            if (getName().equals("mapped-json")) {
                Map<String, String> context = getContext();
                String prefix = spec.getString("prefix", "urn:plain:");
                return new MappedJsonResponseParser(context, prefix);
            } else {
                throw ex("Unsupported response-parser %s", name);
            }
        }

        private @Nonnull Map<String, String> getContext() {
            assert getName().equals("mapped-json");
            Map<String, String> context = new HashMap<>();
            for (Map.Entry<String, Object> e : spec.getMapNN("context").asMap().entrySet()) {
                if ((e.getValue() instanceof String)) {
                    context.put(e.getKey(), e.getValue().toString());
                } else {
                    logger.error("Ignoring non-string URI {} for property {} at {}",
                                 e.getValue(), e.getKey(), spec);
                }
            }
            return context;
        }
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
            String name = paramObj.getString("name", "");
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
                .getString("get/responses/200/x-cardinality", "");
        Cardinality cardinality = Cardinality.parse(string);
        return cardinality != null ? cardinality : fallback;
    }

    private @Nullable SimpleDateSerializer parseDateSerializer(@Nonnull DictTree serializerObj) {
        if (!serializerObj.contains("serializer", "date")) return null;
        String dateFmt = serializerObj.getString("date-format", "");
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
    parseOnlyNumbersSerializer(@Nonnull DictTree serializerObj) {
        if (!serializerObj.contains("serializer", "only-numbers"))
            return null;
        long width = serializerObj.getLong("width", Long.MIN_VALUE);
        if (width != Long.MIN_VALUE && width < 1) {
            logger.warn("Ignoring invalid width {} for OnlyNumbersTermSerializer", width);
            width = Long.MIN_VALUE;
        }
        String fill = serializerObj.getString("fill", "0");
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
        TermSerializer serializer = parseDateSerializer(map);
        if (serializer != null)
            return serializer;
        serializer = parseOnlyNumbersSerializer(map);
        return serializer;
    }

    private @Nullable TermSerializer getTermSerializer(@Nonnull String endpoint,
                                                       @Nonnull DictTree paramObj,
                                                       @Nullable APIDescriptionContext ctx) {
        String name = paramObj.getString("name", "");
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

    private @Nonnull String getTemplateBase(@Nonnull String endpoint) {
        String builder = String.valueOf(swagger.getOrDefault("host", "")) + '/' +
                swagger.getOrDefault("basePath", "") + '/' + endpoint;
        String proto = builder.replaceAll("//+", "/");
        if (!proto.matches("^https?://"))
            proto = getScheme().replaceAll("://$", "") + "://" + proto;
        return proto;
    }

    private @Nonnull
    DictTree getPathObj(@Nonnull String endpoint) {
        String escaped = endpoint.replaceAll("/", "%2F");
        DictTree map = swagger.getMapNN("paths/" + escaped);
        if (map.isEmpty())
            throw ex("Path object not found for endpoint %s", endpoint);
        return map;
    }

    private @Nonnull List<PrimitiveParser> getGlobalParsers() {
        List<PrimitiveParser> list = new ArrayList<>();
        for (Object elem : swagger.getListNN("x-parser")) {
            if (!(elem instanceof DictTree)) continue;
            PrimitiveParser parser = PrimitiveParserParser.parse((DictTree) elem);
            if (parser != null)
                list.add(parser);
        }
        return list;
    }

    @VisibleForTesting
    @Nonnull JsonSchemaMoleculeParser parseSchema(@Nonnull String endpoint) {
        DictTree method = getPathObj(endpoint).getMapNN("get");
        DictTree schema = method.getMapNN("responses/200/schema");
        if (schema.isEmpty())
            throw ex("API path %s has no schema on operation", endpoint);
        JsonSchemaMoleculeParser parser = new JsonSchemaMoleculeParser();
        List<String> mediaTypes = getEndpointProducedMediaTypes(endpoint);
        ProtoResponseParser proto = findResponseParser(method, mediaTypes);
        if (proto == null)
            proto = findResponseParser(swagger, mediaTypes);
        if (proto != null && proto.getName().equals("mapped-json")) {
            Map<String, String> ctx = proto.getContext();
            Map<String, Term> liftedCtx = new HashMap<>();
            for (Map.Entry<String, String> e : ctx.entrySet())
                liftedCtx.put(e.getKey(), new StdURI(e.getValue()));
            parser.setProp2Term(liftedCtx);
            parser.setDefaultPrefix(proto.spec.getString("prefix", "urn:plain:"));
        }
        for (PrimitiveParser pp : getGlobalParsers()) {
            if (pp instanceof DatePrimitiveParser)
                parser.setGlobalDateParser(pp);
        }
        parser.parse(schema);
        return parser;
    }

    private @Nonnull ImmutableSet<String> findEndpoints() {
        DictTree paths = swagger.getMap("paths");
        if (paths == null) {
            logger.warn("No paths in Swagger!");
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String path : paths.keySet()) {
            DictTree op = getPathObj(path);
            if (op.isEmpty()) continue;
            if (op.getMapNN("get/responses/200/schema").isEmpty()) continue;
            builder.add(path);
        }
        return builder.build();
    }

    /* --- --- --- --- --- --- Factory --- --- --- --- --- --- */

    public static class Factory extends AbstractAPIDescriptionParserFactory {
        public @Nonnull SwaggerParser fromDict(@Nonnull DictTree root) {
            Queue<DictTree> queue = new ArrayDeque<>();
            queue.add(root);
            while (!queue.isEmpty()) {
                DictTree obj = queue.remove();
                if (obj.containsKey("swagger"))
                    return new SwaggerParser(obj.asRoot());
                if (obj.containsKey("baseSwagger")) {
                    DictTree base = obj.getMapNN("baseSwagger");
                    DictTree overlay = obj.getMapNN("overlay");
                    return new SwaggerParser(base, overlay);
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
