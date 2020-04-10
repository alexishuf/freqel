package br.ufsc.lapesd.riefederator.util;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillNotClose;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A Tree of {@link java.util.Map} instances. Keys are always strings and non-leaf nodes are Maps.
 *
 * The main use case is abstracting away reading from JSON and YAML. JSON references ("$ref") [1]
 * are implicitly followed.
 *
 * [1]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-02
 */
public class DictTree {
    private static final Logger logger = LoggerFactory.getLogger(DictTree.class);
    private static final Pattern canonizeRx = Pattern.compile("(?i)(%2F|^/+)");

    private @Nullable String name;
    private @Nonnull Map<String, Object> backend;
    private @Nullable DictTree root;

    /* --- --- --- --- Constructors --- --- --- --- */

    public DictTree() {
        this(new HashMap<>());
    }

    public DictTree(@Nonnull Map<String, Object> map) {
        this(map, null);
    }

    public DictTree(@Nonnull Map<String, Object> map, @Nullable String name) {
        this.name = name;
        this.backend = map;
    }
    public DictTree(@Nullable DictTree root, @Nonnull Map<String, Object> map,
                    @Nullable String name) {
        this.name = name;
        this.backend = map;
        this.root = root;
    }

    @SuppressWarnings("unchecked")
    public @Nonnull
    DictTree copy() {
        ArrayDeque<Map<String, Object>> stack = new ArrayDeque<>();
        HashMap<String, Object> rootMap = new HashMap<>(backend);
        stack.push(rootMap);
        while (!stack.isEmpty()) {
            Map<String, Object> map = stack.pop();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                Object value = e.getValue();
                if (value instanceof Map) {
                    HashMap<String, Object> copy = new HashMap<>((Map<String, Object>) value);
                    stack.push(copy);
                    e.setValue(copy);
                } else if (value instanceof Collection) {
                    List<Object> list = new ArrayList<>();
                    for (Object m : ((List<?>) value)) {
                        if (m instanceof Map) {
                            HashMap<String, Object> copy = new HashMap<>((Map<String, Object>) m);
                            list.add(copy);
                            stack.push(copy);
                        } else {
                            list.add(m);
                        }
                    }
                    e.setValue(list);
                }
            }
        }
        return new DictTree(root, rootMap, name);
    }

    /* --- --- --- --- Factory methods --- --- --- --- */

    public static class Loader {
        private @Nullable String name;
        private @Nullable UriResolver uriResolver;

        public Loader(@Nullable String name, @Nullable UriResolver uriResolver) {
            this.name = name;
            this.uriResolver = uriResolver;
        }

        public @WillNotClose @Nonnull
        DictTree
        fromInputStream(@Nonnull InputStream inputStream) throws IOException {
            BufferedInputStream buffered = inputStream instanceof BufferedInputStream
                    ? (BufferedInputStream)inputStream : new BufferedInputStream(inputStream);
            assert buffered.markSupported();
            buffered.mark(Integer.MAX_VALUE);
            InputStreamReader reader = new InputStreamReader(buffered, StandardCharsets.UTF_8);
            boolean json = true;
            for (int value = reader.read(); json && value > -1; value = reader.read()) {
                char c = (char)value;
                if      (c == '{' || c == '[')      break;
                else if (!Character.isSpaceChar(c)) json = false;
            }
            buffered.reset();
            return json ? fromJson(buffered) : fromYaml(buffered);
        }

        public  @Nonnull
        DictTree fromFile(@Nonnull File file) throws IOException {
            UriResolver fallback = this.uriResolver;
            this.uriResolver = new FileResolver(file.getParentFile(), fallback);
            try {
                if (file.getName().toLowerCase().endsWith(".yaml"))
                    return fromYamlFile(file);
                return fromJsonFile(file);
            } finally {
                this.uriResolver = fallback;
            }
        }

        public @Nonnull
        DictTree fromYamlFile(@Nonnull File file) throws IOException {
            if (name == null) name = file.getAbsolutePath();
            try (FileInputStream in = new FileInputStream(file)) {
                return fromYaml(in);
            }
        }

        @WillNotClose
        public @Nonnull
        DictTree fromYaml(@Nonnull InputStream in) throws IOException {
            return fromYaml(new InputStreamReader(in, StandardCharsets.UTF_8));
        }

        @SuppressWarnings("unchecked")
        public @WillNotClose @Nonnull @CheckReturnValue List<DictTree>
        fromYamlList(@Nonnull Reader plainReader) throws IOException {
            YamlReader reader = new YamlReader(plainReader);
            List<DictTree> list = new ArrayList<>();
            for (Object read = reader.read(); read != null; read = reader.read()) {
                if (read instanceof Map) {
                    resolveRefs((Map<String, Object>)read);
                    list.add(new DictTree((Map<String, Object>)read, name));
                }
            }
            return list;
        }

        @WillNotClose @CheckReturnValue
        public @Nonnull DictTree fromYaml(@Nonnull Reader plainReader) throws IOException {
            List<DictTree> list = fromYamlList(plainReader);
            return list.isEmpty() ? new DictTree() : list.get(0);
        }

        @CheckReturnValue
        public @Nonnull DictTree fromYamlString(@Nonnull String yaml) throws IOException {
            if (name == null)
                name = yaml.length() < 20 ? yaml : (yaml.substring(0, 17) + "...");
            return fromYaml(new StringReader(yaml));
        }

        public @Nonnull DictTree fromJsonFile(@Nonnull File file) throws IOException {
            if (name == null) name = file.getAbsolutePath();
            try (FileInputStream in = new FileInputStream(file)) {
                return fromJson(in);
            }
        }

        @WillNotClose @CheckReturnValue @Nonnull
        public DictTree fromJson(@Nonnull InputStream inputStream) throws IOException {
            List<DictTree> list = fromJsonList(inputStream);
            return list.isEmpty() ? new DictTree() : list.get(0);
        }

        @WillNotClose @CheckReturnValue @Nonnull
        public List<DictTree> fromJsonList(@Nonnull InputStream inputStream) throws IOException {
            try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                List<DictTree> list = new ArrayList<>();
                JsonElement r = new JsonParser().parse(reader);
                for (JsonElement e : (r.isJsonArray() ? r.getAsJsonArray() : singletonList(r))) {
                    //noinspection unchecked
                    Map<String, Object> map = new Gson().fromJson(e, LinkedTreeMap.class);
                    resolveRefs(map);
                    list.add(new DictTree(map, name));
                }
                return list;
            }
        }

        @CheckReturnValue
        public @Nonnull DictTree fromJsonString(@Nonnull String json) throws IOException {
            if (name == null)
                name = json.length() <= 20 ? json : (json.substring(0, 17) + "...");
            return fromJson(IOUtils.toInputStream(json, StandardCharsets.UTF_8));
        }

        @CheckReturnValue
        public @Nonnull List<DictTree> fromJsonStringList(@Nonnull String json) throws IOException {
            if (name == null)
                name = json.length() <= 20 ? json : (json.substring(0, 17) + "...");
            return fromJsonList(IOUtils.toInputStream(json, StandardCharsets.UTF_8));
        }

        @CheckReturnValue @Nonnull
        public DictTree fromMap(@Nonnull Map<String, Object> map) throws IOException {
            resolveRefs(map);
            return new DictTree(map, name);
        }

        public class FileResolver implements UriResolver {
            private @Nonnull File referenceDir;
            private @Nullable final UriResolver fallback;

            public FileResolver(@Nonnull File referenceDir, @Nullable UriResolver fallback) {
                this.referenceDir = referenceDir;
                this.fallback = fallback;
            }

            @Override
            public @Nonnull DictTree load(@Nonnull String uri) throws IOException {
                if (uri.matches("^\\w+://.*"))
                    return fallback != null ? fallback.load(uri) : fromUri(uri);
                File file = new File(referenceDir, uri.replaceAll("^/+", ""));
                if (!file.exists() && fallback != null)
                    return fallback.load(uri);
                return fromFile(file);
            }
        }

        public class ResourceResolver implements UriResolver {
            private String basePath;
            private @Nullable UriResolver fallback;

            public ResourceResolver(@Nonnull String resourcePath, @Nullable UriResolver fallback) {
                this.basePath = resourcePath.replaceAll("/?[^/]+$", "").replaceAll("^/+", "")
                                            .replaceAll("/+$", "");
                this.fallback = fallback;
            }
            public ResourceResolver(@Nonnull Class<?> cls, @Nonnull String relativePath,
                                    @Nullable UriResolver fallback) {
                basePath = (cls.getName().replaceAll("\\.[^.]+$", "")
                                        .replace('.', '/')
                         + "/" + relativePath.replaceAll("/?[^/]+$", "").replaceAll("^/+", "")
                ).replaceAll("/+$", "");
                this.fallback = fallback;
            }

            @Override
            public @Nonnull DictTree load(@Nonnull String uri) throws IOException {
                assert !basePath.startsWith("/") && !basePath.endsWith("/");
                if (!uri.startsWith("/") && fallback != null)
                    return fallback.load(uri);
                if (!uri.startsWith("/"))
                    uri = "/" + uri; //fallback should process this, but we have no fallback...
                return fromResource(basePath + uri);
            }
        }

        public @Nonnull DictTree fromResource(String resourcePath) throws IOException {
            UriResolver fallback = this.uriResolver;
            this.uriResolver = new ResourceResolver(resourcePath, fallback);
            if (this.name == null) this.name = resourcePath;
            try (InputStream stream = ResourceOpener.getStream(resourcePath)) {
                return fromInputStream(stream);
            } finally {
                this.uriResolver = fallback;
            }
        }

        public @Nonnull DictTree
        fromResource(@Nonnull Class<?> cls, @Nonnull String resourcePath) throws IOException {
            UriResolver fallback = this.uriResolver;
            this.uriResolver = new ResourceResolver(cls, resourcePath, fallback);
            if (this.name == null) {
                this.name = cls.getName().replace('.', '/')
                                         .replaceAll("/[^/]+$", "") + "/" + resourcePath;
            }
            try (InputStream stream = ResourceOpener.getStream(cls, resourcePath)) {
                return fromInputStream(stream);
            } finally {
                uriResolver = fallback;
            }
        }

        public @Nonnull DictTree fromUri(@Nonnull String uri) throws IOException {
            if (uri.startsWith("file://"))
                return fromFile(new File(uri.replaceAll("^file://", "")));
            try (InputStream in = new URL(uri).openStream()) {
                return fromInputStream(in);
            }
        }

        @SuppressWarnings("unchecked")
        private void resolveRefs(@Nonnull Map<String, Object> map) throws IOException {
            if (uriResolver != null) {
                for (Map.Entry<String, Object> e : map.entrySet()) {
                    if (!(e.getValue() instanceof Map)) continue;
                    Map<String, Object> value = (Map<String, Object>) e.getValue();
                    if (value.keySet().equals(singleton("$ref"))) {
                        Object ref = value.get("$ref");
                        if (ref instanceof String && !ref.toString().startsWith("#"))
                            e.setValue(uriResolver.load(ref.toString()).backend);
                    } else {
                        resolveRefs(value);
                    }
                }
            }
        }
    }

    @FunctionalInterface
    public interface UriResolver {
        @Nonnull
        DictTree load(@Nonnull String uri) throws IOException;
    }

    public static @Nonnull Loader load(@Nullable String name, @Nullable UriResolver uriResolver) {
        return new Loader(name, uriResolver);
    }

    public static @Nonnull Loader load(@Nonnull UriResolver uriResolver) {
        return new Loader(null, uriResolver);
    }

    public static @Nonnull Loader load(@Nonnull String name) {
        return new Loader(name, null);
    }

    public static @Nonnull Loader load() {
        return new Loader(null, null);
    }

    /* --- --- --- --- utils --- --- --- --- */


    @SuppressWarnings("unchecked")
    public static @Nonnull
    DictTree override(@Nonnull DictTree overridden,
                      @Nonnull Object overriding,
                      @Nullable String idPath) {
        if (overriding instanceof DictTree) {
            return overrideObject(overridden, (DictTree) overriding);
        } else if ((overriding instanceof Collection) && idPath != null && !idPath.isEmpty()) {
            Object id = overridden.get(idPath);
            for (Object cand : (Collection<Object>) overriding) {
                if (!(cand instanceof DictTree)) continue;
                DictTree map = (DictTree) cand;
                if (Objects.equals(id, map.get(idPath))) {
                    return overrideObject(overridden, map);
                }
            }
        }
        return overridden;
    }

    private static @Nonnull
    DictTree overrideObject(@Nonnull DictTree overridden,
                            @Nonnull DictTree map) {
        Map<String, Object> result = new HashMap<>();
        for (String key : map.keySet()) {
            Object value = map.get(key);
            if (value != null)
                result.put(key, value);
        }
        for (String key : overridden.keySet()) {
            if (result.containsKey(key)) continue;
            result.put(key, overridden.get(key));
        }
        return new DictTree(overridden.root, result, overridden.name);
    }

    @SuppressWarnings("unchecked")
    public static @Nonnull List<Object> override(@Nonnull List<?> overridden,
                                                 @Nonnull Object overriding,
                                                 @Nullable String idPath) {
        List<Object> result = new ArrayList<>(overridden.size());
        if (idPath != null && idPath.isEmpty()) idPath = null;

        Set<Object> selectedIds = new HashSet<>(overridden.size());
        for (Object o : overridden) {
            if (o instanceof DictTree) {
                DictTree map = (DictTree) o;
                if (idPath != null) {
                    Object id = map.get(idPath);
                    if (id != null)
                        selectedIds.add(id);
                }
                result.add(override(map, overriding, idPath));
            } else {
                result.add(o);
            }
        }

        Collection<Object> candidates = overriding instanceof Collection
                ? (Collection<Object>)overriding : Collections.singletonList(overriding);
        for (Object candidate : candidates) {
            if (!(candidate instanceof DictTree)) continue;
            if (idPath == null || !selectedIds.contains(((DictTree)candidate).get(idPath)))
                result.add(candidate);
        }

        return result;
    }

    private static @Nullable String currentIdProp(@Nullable Object current,
                                                  @Nonnull Collection<List<String>> idPaths,
                                                  @Nonnull List<String> path) {
        assert idPaths.stream().noneMatch(l -> l.size() <= 1);
        if (path.isEmpty() || current == null)
            return null; //empty path should not match
        outer:
        for (List<String> idPath : idPaths) {
            List<String> parentIdPath = idPath.subList(0, idPath.size()-1);
            ListIterator<String> i1 = parentIdPath.listIterator(parentIdPath.size());
            ListIterator<String> i2 = path.listIterator(path.size());
            while (i1.hasPrevious() && i2.hasPrevious()) {
                if (!i1.previous().equals(i2.previous()))
                    continue outer;
            }
            if (!i1.hasPrevious()) {// idPath fully matched
                return idPath.get(idPath.size()-1);
            }
        }
        return null; //no match
    }

    @SuppressWarnings("unchecked")
    private static boolean sameId(@Nonnull String idProp, @Nonnull Object left,
                                  @Nonnull Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> lm = (Map<String, Object>) left;
            Map<String, Object> rm = (Map<String, Object>) right;
            Object li = lm.get(idProp), ri = rm.get(idProp);
            return Objects.equals(li, ri) && li != null;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> overlayLists(@Nonnull List<String> path,
                                             @Nonnull Collection<List<String>> idPaths,
                                             @Nullable String idProp, Object old, Object novel) {
        List<?> ol = old   instanceof List ? (List<?>)old   : singletonList(old);
        List<?> nl = novel instanceof List ? (List<?>)novel : singletonList(novel);
        ArrayList<Object> list = new ArrayList<>(ol);
        if (idProp != null) {
            BitSet selected = new BitSet(nl.size());
            for (Object oldMember : ol) {
                for (int i = 0; i < nl.size(); i++) {
                    Object cand = nl.get(i);
                    if (sameId(idProp, oldMember, cand)) {
                        selected.set(i);
                        overlay((Map<String, Object>)oldMember,
                                (Map<String, Object>)cand, path, idPaths);
                    }
                }
            }
            for (int i = selected.nextClearBit(0); i < nl.size();
                 i = selected.nextClearBit(i+1)) {
                list.add(nl.get(i));
            }
        } else {
            list.addAll(nl);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    private static void overlay(@Nonnull Map<String, Object> target,
                                @Nonnull Map<String, Object> source,
                                @Nonnull List<String> path,
                                @Nonnull Collection<List<String>> idPaths) {
        for (String key : source.keySet()) {
            Object novel = source.get(key);
            if (target.containsKey(key)) {
                Object old = target.get(key);
                path.add(key);
                String idProp = currentIdProp(old, idPaths, path);

                if (old instanceof List || novel instanceof List) {
                    target.put(key, overlayLists(path, idPaths, idProp, old, novel));
                } else if (old instanceof Map && novel instanceof Map) {
                    Map<String, Object> oldMap = (Map<String, Object>) old;
                    if (idProp == null || sameId(idProp, old, novel))
                        overlay(oldMap, (Map<String, Object>) novel, path, idPaths);
                } else {
                    target.put(key, novel);
                }
                path.remove(path.size()-1);
            } else {
                target.put(key, novel);
            }
        }
    }

    public static @Nonnull DictTree overlay(@Nonnull DictTree target,
                                            @Nonnull DictTree source,
                                            @Nonnull Collection<List<String>> idPaths) {
        Preconditions.checkArgument(idPaths.stream().allMatch(p -> p.size() > 1),
                                    "All idPaths must have at least two segments");
        DictTree copy = target.copy();
        overlay(copy.backend, source.backend, new ArrayList<>(), idPaths);
        return copy.asRoot();
    }

    public static @Nonnull DictTree overlay(@Nonnull DictTree target,
                                            @Nonnull DictTree source,
                                            @Nonnull String... idPaths) {
        List<List<String>> idPathLists = new ArrayList<>(idPaths.length);
        for (String idPath : idPaths) {
            List<String> l = Splitter.on('/').omitEmptyStrings().splitToList(idPath);
            idPathLists.add(l.stream().map(s -> canonizeRx.matcher(s).replaceAll("/"))
                                      .collect(toList()));
        }
        return overlay(target, source, idPathLists);
    }

    public static @Nonnull DictTree overlay(@Nonnull DictTree target,
                                            @Nonnull DictTree source) {
        return overlay(target, source, emptyList());
    }

    /* --- --- --- --- Accessors --- --- --- --- */

    public @Nonnull Map<String, Object> asMap() {
        return backend;
    }

    public @Nullable String getName() {
        return name;
    }

    public @Nonnull Set<String> keySet() {
        return backend.keySet();
    }

    public boolean containsKey(@Nonnull String key) {
        return backend.containsKey(key);
    }

    public boolean isEmpty() {
        return backend.isEmpty();
    }

    public int size() {
        return backend.size();
    }

    public @Nonnull
    DictTree asRoot() {
        return new DictTree(backend, name);
    }

    /**
     * Tests if this map contains the expected value at the path.
     *
     * If <code>expected</code> is a {@link Collection}, test if it is a subset of the
     * actual collection at the path. Else, if the actual value at the path is a
     * {@link Collection}, test if <code>expected</code> is a member.
     *
     * @param path path to fetch
     * @param expected expected value or subset (Collection)
     * @return true iff expected is contained in value at the path
     */
    public boolean contains(String path, @Nonnull Object expected) {
        Object actual = getOrDefault(path, null);
        if (actual instanceof Collection) {
            Collection<?> actualColl = (Collection<?>) actual;
            if (expected instanceof Collection) {
                return ((Collection<?>)expected).stream()
                        .allMatch(e -> actualColl.stream().anyMatch(a -> tolerantEquals(e, a)));
            } else {
                return actualColl.stream().anyMatch(a -> tolerantEquals(a, expected));
            }
        }
        return tolerantEquals(actual, expected);
    }

    public @Nullable Object put(@Nonnull String key, @Nullable Object value) {
        return getChild(backend.put(key, value), key);
    }

    public @Nullable Object remove(@Nonnull String key) {
        return getChild(backend.remove(key), key);
    }

    @SuppressWarnings("unchecked") @CheckReturnValue @Contract("_, !null -> !null")
    public Object getOrDefault(@Nonnull String path, Object fallback) {
        List<String> steps = Splitter.on('/').omitEmptyStrings().splitToList(path);
        if (steps.isEmpty()) {
            logger.info("getOrDefault() with empty path does not make sense, returning this");
            return this;
        }

        Object object = backend;
        for (String step : steps) {
            if (step.equals("#")) continue;

            // Try to coerce object into a Map<String, Object>
            if (object instanceof List && !((List<?>)object).isEmpty())
                object = ((List<?>)object).get(0);
            if (object instanceof DictTree)
                object = ((DictTree) object).backend;
            if (!(object instanceof Map)) { //could not coerce
                logger.info("Cannot get {} (path {}) from {}: current element not a map",
                        step, path, this);
                return fallback;
            }

            Map<String, Object> asMap = (Map<String, Object>) object;
            // try to get step as is
            if (!asMap.containsKey(step)) {
                // try expanding escaped "%2F"'s ...
                step = canonizeRx.matcher(step).replaceAll("/");
                if (!asMap.containsKey(step)) {
                    // the Splitter ate up leading '/'s and/or the input is escaped
                    for (String key : asMap.keySet()) {
                        if (canonizeRx.matcher(key).replaceAll("").equals(step)) {
                            step = key;
                            break;
                        }
                    }
                }
            }
            if (!asMap.containsKey(step)) {
                logger.debug("Current element does not have property {} (path {})", step, path);
                return fallback;
            }
            //get value for step and pass through getChild (process $ref)
            object = asMap.getOrDefault(step, null);
            object = getChild(object, path);
        }
        return object == null ? fallback : object;
    }
    @CheckReturnValue
    public @Nullable Object get(@Nonnull String path) {
        return getOrDefault(path, null);
    }

    @CheckReturnValue
    public @Nullable
    DictTree getMapOrDefault(@Nonnull String path,
                             @Nullable DictTree fallback) {
        Object value = get(path);
        if (value == null) return fallback;
        if (value instanceof List) {
            List<?> list = (List<?>)value;
            if (list.size() == 1 && list.get(0) instanceof DictTree)
                return (DictTree) list.get(0);
        }
        if (value instanceof DictTree)
            return (DictTree) value;
        logger.info("Expected a map as value of {}. Got {}", path, value);
        return fallback;
    }
    @CheckReturnValue
    public @Nullable
    DictTree getMap(@Nonnull String path) {
        return getMapOrDefault(path, null);
    }

    @SuppressWarnings("unchecked")
    public @Nullable List<Object> getList(@Nonnull String path) {
        Object o = getOrDefault(path, null);
        if (o == null) return null;
        if (o instanceof List)
            return (List<Object>)o;
        return Collections.singletonList(o);
    }

    public @Nonnull List<Object> getListNN(@Nonnull String path) {
        List<Object> list = getList(path);
        return list == null ? Collections.emptyList() : list;
    }

    public Object getPrimitive(@Nonnull String path, Object fallback) {
        Object value = get(path);
        if (value instanceof List && !((List<?>)value).isEmpty())
            value = ((List<?>)value).get(0);
        if (value instanceof DictTree)
            value = getOrDefault("value", fallback);
        return value == null ? fallback : value;
    }

    @Contract("_, !null -> !null")
    public String getString(@Nonnull String path, String fallback) {
        Object value = getPrimitive(path, null);
        if (value == null)
            return fallback;
        return value.toString();
    }

    public long getLong(@Nonnull String path, long fallback) {
        Object value = getPrimitive(path, null);
        if (value instanceof String && ((String)value).matches("\\d+"))
            value = Long.parseLong((String) value);
        if (value instanceof Number)
            return ((Number) value).longValue();
        return fallback;
    }

    public double getDouble(@Nonnull String path, double fallback) {
        Object value = getPrimitive(path, null);
        if (value instanceof String && ((String)value).matches("\\d*(\\.\\d*)?"))
            value = Double.parseDouble((String) value);
        if (value instanceof Number)
            return ((Number) value).doubleValue();
        return fallback;
    }

    public boolean getBoolean(@Nonnull String path, boolean fallback) {
        return Boolean.parseBoolean(getString(path, Boolean.toString(fallback)));
    }

    @CanIgnoreReturnValue
    public @Nonnull Object getNN(@Nonnull String path) throws NoSuchElementException {
        Object value = get(path);
        if (value == null) {
            String mes = String.format("No value value for path %s in %s", path, this);
            throw new NoSuchElementException(mes);
        }
        return value;
    }
    @CanIgnoreReturnValue
    public @Nonnull
    DictTree getMapNN(@Nonnull String path) {
        DictTree value = getMap(path);
        if (value == null)
            logger.debug("No value for path {}. Expected an object", path);
        return value == null ? (DictTree)requireNonNull(getChild(emptyMap(), path)) : value;
    }

    /* --- --- --- --- private methods --- --- --- --- */

    private @Nonnull
    DictTree getRoot() {
        return root == null ? this : root;
    }

    @SuppressWarnings("unchecked")
    private @Nullable Object getChild(@Nullable Object object, @Nonnull String path) {
        if (object == null) return null;
        if (object instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) object;
            if (map.containsKey("$ref")) {
                return getRoot().get(map.get("$ref").toString());
            } else {
                StringBuilder b = new StringBuilder();
                if (name != null) b.append(name);
                return new DictTree(getRoot(), map, b.append('/').append(path).toString());
            }
        }
        if (object instanceof List) {
            List<?> list = (List<?>) object;
            if (list.stream().anyMatch(Map.class::isInstance)) {
                ArrayList<Object> copy = new ArrayList<>();
                for (Object o : list)
                    copy.add(getChild(o, path));
                return copy;
            }
        }
        return object;
    }

    /* --- --- --- --- Object methods --- --- --- --- */

    public boolean tolerantEquals(@Nullable Object o) {
        if (o == null) return false;
        if (o instanceof DictTree)
            return backend.equals(((DictTree) o).backend);
        if (o instanceof Map)
            return backend.equals(o);
        if (o instanceof Collection && ((Collection<?>)o).size() == 1)
            return backend.equals(((Collection<?>)o).iterator().next());
        return false;
    }

    public static boolean tolerantEquals(@Nullable Object l, @Nullable Object r) {
        if ((l == null) ^  (r == null)) return false;
        if (l == null) return true;
        if (l instanceof DictTree) return ((DictTree) l).tolerantEquals(r);
        if (r instanceof DictTree) return ((DictTree) r).tolerantEquals(l);
        if (l.equals(r))
            return true;
        if (l instanceof Double || l instanceof Float) {
            double dv = ((Number) l).doubleValue();
            if (Math.floor(dv) == dv) l = (long) dv;
        }
        if (r instanceof Double || r instanceof Float) {
            double dv = ((Number) r).doubleValue();
            if (Math.floor(dv) == dv) r = (long) dv;
        }
        return l.toString().equals(r.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Map)
            return backend.equals(o);
        if (!(o instanceof DictTree))
            return false;
        DictTree dictTree = (DictTree) o;
        return Objects.equals(name, dictTree.name) &&
                              backend.equals(dictTree.backend);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, backend);
    }

    @Override
    public String toString() {
        return name == null ? super.toString() : String.format("HierarchicalMap(%s)", name);
    }
}
