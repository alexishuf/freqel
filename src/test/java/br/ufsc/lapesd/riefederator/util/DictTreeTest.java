package br.ufsc.lapesd.riefederator.util;

import com.google.common.base.Splitter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class DictTreeTest {

    private final String yamlWithSlashProperty =
            "root:\n" +
            "  level1:\n" +
            "    x/y:\n" +
            "      z: 20\n" +
            "    a%2Fb:\n" +
            "      z: 30\n";

    @DataProvider
    public static Object[][] testPathData() {
        return Stream.of(
                asList("a_string", "\"asd\""),
                asList("lom1", "{\"x\": 1}"),
                asList("lom2", "[23, {\"x\": 1}, \"bogus\", {\"x\": 3}]"),
                asList("tree", "{\"leaf\": {\"x\": 4}}"),
                asList("tree/leaf", "{\"x\": 4}"),
                asList("tree//leaf", "{\"x\": 4}"),
                asList("tree/leaf/", "{\"x\": 4}"),
                asList("/tree/leaf", "{\"x\": 4}"),
                asList("#/tree/leaf", "{\"x\": 4}"),
                asList("#//tree//leaf/", "{\"x\": 4}"),
                asList("child/grandchild/backref1", "{\"x\": 1}"),
                asList("child/grandchild/backref3", "{\"x\": 4}"),
                asList("child/grandchild/backref3/x", "4"),
                asList("child/grandchild/backreft", "{\"leaf\": {\"x\": 4}}"),
                asList("child/grandchild/backreft/leaf", "{\"x\": 4}"),
                asList("child/grandchild/backrefx", "4")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testPathData")
    public void testJsonStream(String path, String jsonExpected) throws IOException {
        DictTree map;
        try (InputStream inputStream = getClass().getResourceAsStream("test.json")) {
            map = DictTree.load("test.json").fromJson(inputStream);
        }
        doPathTest(map, path, jsonExpected);
    }

    @Test(dataProvider = "testPathData")
    public void testYamlStream(String path, String jsonExpected) throws IOException {
        DictTree map;
        try (InputStream inputStream = getClass().getResourceAsStream("test.yaml")) {
            map = DictTree.load("test.yaml").fromYaml(inputStream);
        }
        doPathTest(map, path, jsonExpected);
    }

    @Test(dataProvider = "testPathData")
    public void testYamlDocBreakStream(String path, String jsonExpected) throws IOException {
        DictTree map;
        try (InputStream inputStream = getClass().getResourceAsStream("test+doc_break.yaml")) {
            map = DictTree.load("test+doc_break.yaml").fromYaml(inputStream);
        }
        doPathTest(map, path, jsonExpected);
    }

    @Test(dataProvider = "testPathData")
    public void testAutoJsonStream(String path, String jsonExpected) throws IOException {
        DictTree map;
        try (InputStream inputStream = getClass().getResourceAsStream("test.json")) {
            map = DictTree.load("test").fromInputStream(inputStream);
        }
        doPathTest(map, path, jsonExpected);
    }

    @Test(dataProvider = "testPathData")
    public void testAutoYamlStream(String path, String jsonExpected) throws IOException {
        DictTree map;
        try (InputStream inputStream = getClass().getResourceAsStream("test.yaml")) {
            map = DictTree.load("test").fromInputStream(inputStream);
        }
        doPathTest(map, path, jsonExpected);
    }


    private void doPathTest(DictTree map, @Nonnull String path,
                            @Nullable String jsonExpected) {
        assertNotNull(map);
        if (jsonExpected == null) {
            assertNull(map.get(path));
            assertNull(map.getMap(path));

            expectThrows(NoSuchElementException.class, () -> map.getNN(path));

            DictTree empty = map.getMapNN(path);
            assertNotNull(empty);
            assertTrue(empty.isEmpty());
        } else {
            assertNotNull(map.get(path));
            assertEquals(map.get(path), map.getNN(path));

            if (jsonExpected.startsWith("{")) {
                Object o = map.get(path);
                if (o instanceof List) {
                    assertEquals(o, singletonList(map.getMap(path)));
                    assertEquals(o, singletonList(map.getMapNN(path)));
                } else {
                    assertEquals(o, map.getMap(path));
                    assertEquals(o, map.getMapNN(path));
                }
            }

            if (jsonExpected.matches("\".*\"")) {
                assertTrue(map.contains(path, jsonExpected.replaceAll("\"(.*)\"", "$1")));
            } else if (jsonExpected.matches("\\d+")) {
                long expected = Long.parseLong(jsonExpected);
                assertTrue(map.contains(path, expected));
                assertEquals(map.getLong(path, -999), expected);
                assertEquals(map.getDouble(path, -999), (double) expected);
            } else if (jsonExpected.matches("\\d+\\.\\d+")) {
                double expected = Double.parseDouble(jsonExpected);
                assertTrue(map.contains(path, expected));
                assertEquals(map.getDouble(path, -999.0), expected);
            }

            JsonParser jsonParser = new JsonParser();
            JsonElement element = jsonParser.parse(jsonExpected);
            assertNotNull(element);
            checkJson(element, map.get(path), path);

        }
    }

    @SuppressWarnings("unchecked")
    private void checkJson(JsonElement element, Object o, @Nonnull String path) {
        String failMsg = "value mismatch at path " + path;
        if (element == null) {
            assertNull(o, failMsg);
            return;
        }
        assertNotNull(o, failMsg);
        if (element.isJsonPrimitive()) {
            if (element.getAsJsonPrimitive().isNumber()) {
                assertEquals(element.getAsDouble(), Double.parseDouble(o.toString()), failMsg);
            } else {
                assertEquals(element.getAsString(), o.toString(), failMsg);
            }
        } else if (element.isJsonArray()) {
            JsonArray jsonArray = element.getAsJsonArray();
            assertTrue(o instanceof List);
            List<Object> list = (List<Object>) o;
            assertEquals(jsonArray.size(), list.size(), failMsg);

            Iterator<Object> it = list.iterator();
            for (JsonElement e : jsonArray)
                checkJson(e, it.next(), path);
        } else if (element.isJsonObject()) {
            JsonObject jsonObject = element.getAsJsonObject();
            DictTree map;
            if (o instanceof List) {
                List<?> list = (List<?>)o;
                assertEquals(list.size(), 1);
                assertTrue(list.get(0) instanceof DictTree);
                map = (DictTree)list.get(0);
            } else {
                assertTrue(o instanceof DictTree);
                map = (DictTree)o;
            }

            assertEquals(jsonObject.size(), map.size(), failMsg);
            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                Object value = map.get(entry.getKey());
                checkJson(entry.getValue(), value, path+"/"+entry.getKey());
            }
        }
    }

    @Test
    public void testParseSwaggerJsonAsDictTree() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String resourcePath = "br/ufsc/lapesd/riefederator/webapis/portal_transparencia.json";
        try (InputStream in = cl.getResourceAsStream(resourcePath)) {
            assertNotNull(in);
            DictTree tree = DictTree.load().fromInputStream(in);
            String path = "paths/%2Fapi-de-dados%2Flicitacoes/get/responses/200" +
                    "/schema/items/properties/dataAbertura/type";
            assertEquals(tree.getPrimitive(path, ""), "string");
        }
    }

    @Test
    public void testParseSwaggerYamlAsDictTree() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String resourcePath = "br/ufsc/lapesd/riefederator/webapis/nyt_books.yaml";
        try (InputStream in = cl.getResourceAsStream(resourcePath)) {
            assertNotNull(in);
            DictTree tree = DictTree.load().fromInputStream(in);
            String path = "paths/%2Flists.json/get/responses/200" +
                    "/schema/properties/copyright/type";
            assertEquals(tree.getPrimitive(path, ""), "string");
        }
    }

    @Test
    public void testParseOverlaidSwaggerJsonAsDictTree() throws IOException {
        String dirPath = "br/ufsc/lapesd/riefederator/webapis";
        String resourcePath = dirPath + "/portal_transparencia-ext.yaml";
        DictTree tree = DictTree.load().fromResource(resourcePath);

        DictTree base = tree.getMapNN("baseSwagger");
        DictTree overlay = tree.getMapNN("overlay");
        DictTree result = DictTree.overlay(base, overlay, "parameters/name");
        assertEquals(result.getPrimitive("swagger", ""), "2.0");
        assertEquals(result.getPrimitive("x-paging/param", ""), "pagina");

        assertEquals(result.getPrimitive("paths/%2Fapi-de-dados%2Flicitacoes/get" +
                                         "/responses/200/x-cardinality", ""),
                     "LOWER_BOUND(3)");

        List<Object> list = result.getListNN("paths/%2Fapi-de-dados%2Flicitacoes" +
                                             "/get/parameters");
        assertEquals(list.size(), 4);
        DictTree param = list.stream()
                             .filter(o -> "dataInicial".equals(((DictTree) o).get("name")))
                             .map(o -> (DictTree)o)
                             .findFirst().orElse(null);
        assertNotNull(param);
        assertEquals(param.get("in"), "query");
        assertEquals(param.get("x-serializer/serializer"), "date");
    }


    @Test
    public void testUrlEscapes() throws IOException {
        DictTree map = DictTree.load().fromYamlString(yamlWithSlashProperty);
        assertEquals(map.getLong("root/level1/x%2Fy/z", 0), 20);
        assertEquals(map.getLong("root/level1/x%2fy/z", 0), 20);
    }

    @Test
    public void testUrlEscapesInJson() throws IOException {
        DictTree map = DictTree.load().fromYamlString(yamlWithSlashProperty);
        // property "a/b" has the slash escaped in the yaml
        assertEquals(map.getLong("root/level1/a%2Fb/z", 0), 30);
    }

    @Test
    public void testGetListNN() throws IOException {
        DictTree map = DictTree.load().fromJsonString("{" +
                "  \"a\": 1," +
                "  \"b\": [2, 3]," +
                "  \"c\": {\"x\": [1, 2, 3]}," +
                "  \"d\": [1, {\"x\": 2}]" +
                "}");
        //query a list
        assertEquals(map.getListNN("b").stream()
                        .map(n -> ((Number)n).intValue()).collect(toList()),
                     asList(2, 3));
        //not a list, becomes a singletonList
        assertEquals(map.getListNN("a").stream()
                        .map(n -> ((Number)n).intValue()).collect(toList()),
                     singletonList(1));
        //missing property yields empty list
        assertEquals(map.getListNN("x"), emptyList());

        // getting a list preserves object as members
        List<Object> list = map.getListNN("c");
        assertEquals(list.size(), 1);
        assertEquals(((DictTree)list.get(0)).getListNN("x").stream()
                                           .map(n -> ((Number)n).intValue()).collect(toList()),
                     asList(1, 2, 3));

        //getting a list with mixed number/object
        list = map.getListNN("d");
        assertEquals(list.size(), 2);
        assertEquals(((Number)list.get(0)).intValue(), 1);
        assertEquals(((DictTree)list.get(1)).getLong("x", 0), 2L);
    }

    @Test
    public void testOverrideObjectWithObject() throws IOException {
        DictTree a = DictTree.load().fromJsonString("{\"x\": \"1\", \"y\": \"2\"}");
        DictTree b = DictTree.load().fromJsonString("{\"z\": \"3\", \"y\": \"4\"}");
        DictTree c = DictTree.override(a, b, null);

        assertEquals(c.get("x"), "1");
        assertEquals(c.get("y"), "4");
        assertEquals(c.get("z"), "3");
    }

    @Test
    public void testOverrideObjectWithList()  throws IOException {
        DictTree a = DictTree.load()
                .fromJsonString("{\"n\": \"1\", \"x\": \"1\", \"y\": \"2\"}");
        List<DictTree> b = asList(
                DictTree.load().fromJsonString("{\"n\": \"3\", \"z\": \"3\", \"y\": \"4\"}"),
                DictTree.load().fromJsonString("{\"n\": \"1\", \"z\": \"5\", \"y\": \"6\"}"),
                DictTree.load().fromJsonString("{\"n\": \"2\", \"z\": \"7\", \"y\": \"8\"}")
        );
        DictTree c = DictTree.override(a, b, "n");

        assertEquals(c.get("n"), "1");
        assertEquals(c.get("x"), "1");
        assertEquals(c.get("y"), "6");
        assertEquals(c.get("z"), "5");

        DictTree d = DictTree.override(a, b, null);
        assertEquals(d.get("n"), "1");
        assertEquals(d.get("x"), "1");
        assertEquals(d.get("y"), "2");
        assertNull(d.get("z"));
    }

    @Test
    public void testOverrideListWithList() throws IOException {
        List<DictTree> a = asList(
                DictTree.load().fromJsonString("{\"n\": \"1\", \"x\": \"1\", \"y\": \"2\"}"),
                DictTree.load().fromJsonString("{\"n\": \"4\", \"x\": \"3\", \"y\": \"4\"}")
        );
        List<DictTree> b = asList(
                DictTree.load().fromJsonString("{\"n\": \"3\", \"z\": \"3\", \"y\": \"4\"}"),
                DictTree.load().fromJsonString("{\"n\": \"1\", \"z\": \"5\", \"y\": \"6\"}")
        );

        List<Object> c = DictTree.override(a, b, "n");
        assertEquals(c.size(), 3);
        assertEquals(((DictTree)c.get(0)).get("n"), "1");
        assertEquals(((DictTree)c.get(0)).get("x"), "1");
        assertEquals(((DictTree)c.get(0)).get("y"), "6");
        assertEquals(((DictTree)c.get(0)).get("z"), "5");

        assertEquals(((DictTree)c.get(1)).get("n"), "4");
        assertEquals(((DictTree)c.get(1)).get("x"), "3");
        assertEquals(((DictTree)c.get(1)).get("y"), "4");
        assertNull(((DictTree) c.get(1)).get("z"));

        assertEquals(((DictTree)c.get(2)).get("n"), "3");
        assertNull(((DictTree) c.get(2)).get("x"));
        assertEquals(((DictTree)c.get(2)).get("y"), "4");
        assertEquals(((DictTree)c.get(2)).get("z"), "3");

        List<Object> d = DictTree.override(a, b, null);
        assertEquals(d.size(), 4);
        assertEquals(d, asList(a.get(0), a.get(1), b.get(0), b.get(1)));
    }

    @Test
    public void testOverlay() throws IOException {
        DictTree target = DictTree.load().fromJsonString("{" +
                "  \"x\": {" +
                "    \"x1\": [\"1\", \"2\"], " +
                "    \"x2\": [\"1\", \"2\"], " +
                "    \"x3\": \"3\"," +
                "    \"x4\": \"4\"" +
                "  }" +
                "}");
        DictTree source = DictTree.load().fromYamlString("" +
                "x:\n" +
                "  x1:\n" +
                "    y1: 1\n" +
                "  x2:\n" +
                "    - 3\n" +
                "    - 4\n" +
                "  x4:\n" +
                "    - 5\n" +
                "  x5: false");
        DictTree result = DictTree.overlay(target, source);
        assertEquals(result.getMapNN("x").keySet(), newHashSet("x1", "x2", "x3", "x4", "x5"));

        List<Object> list = result.getListNN("x/x1");
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).toString(), "1");
        assertEquals(list.get(1).toString(), "2");
        assertTrue(list.get(2) instanceof DictTree);
        assertEquals(((DictTree)list.get(2)).keySet(), singleton("y1"));
        assertEquals(((DictTree)list.get(2)).get("y1"), "1");

        list = result.getListNN("x/x2").stream().map(Object::toString).collect(toList());
        assertEquals(list, asList("1", "2", "3", "4"));

        assertEquals(result.getLong("x/x3", -1), 3L);

        list = result.getListNN("x/x4").stream().map(Object::toString).collect(toList());
        assertEquals(list, asList("4", "5"));

        assertEquals(result.getPrimitive("x/x5", "").toString(), "false");
    }


    @Test
    public void testIdPathOnListOverlay() throws IOException {
        DictTree base = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    parameters:\n" +
                "      - name: a\n" +
                "        type: string\n" +
                "      - name: b\n" +
                "        type: number\n");
        DictTree overlay = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    parameters:\n" +
                "      - name: a\n" +
                "        extra: 2\n" +
                "    extra: 3\n");

        List<String> idPaths = asList("parameters/name", "x/parameters/name",
                                      "paths/x/parameters/name");
        for (String idPath : idPaths) {
            DictTree result = DictTree.overlay(base, overlay, idPath);

            List<Object> params = result.getListNN("paths/x/parameters");
            assertEquals(params.size(), 2);
            assertEquals(((DictTree)params.get(0)).getPrimitive("name", "").toString(), "a");
            assertEquals(((DictTree)params.get(0)).getPrimitive("extra", "").toString(), "2");
            assertEquals(((DictTree)params.get(1)).getPrimitive("name", "").toString(), "b");
            assertFalse(((DictTree)params.get(1)).containsKey("extra"));

            assertEquals(result.getPrimitive("paths/x/extra", "").toString(), "3");
            assertNull(result.get("paths/extra"));
            assertNull(result.get("extra"));
        }

        List<List<String>> parseIdPaths;
        parseIdPaths = idPaths.stream().map(Splitter.on('/')::splitToList).collect(toList());
        DictTree result = DictTree.overlay(base, overlay, parseIdPaths);

        List<Object> params = result.getListNN("paths/x/parameters");
        assertEquals(params.size(), 2);
        assertEquals(result.getPrimitive("paths/x/extra", "").toString(), "3");
        assertNull(result.get("paths/extra"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCopy() throws IOException {
        DictTree a = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    name: a\n" +
                "    value: 1\n" +
                "    values: [2, 3]");
        DictTree b = a.copy();
        Map<String, Object> x = (Map<String, Object>) ((Map<String, Object>) a.asMap()
                .get("paths")).get("x");
        x.put("extra", 4);
        ((List<Integer>)x.get("values")).add(5);

        // a was affected
        assertEquals(a.getLong("paths/x/extra", 0), 4);
        assertEquals(a.getListNN("paths/x/values").size(), 3);

        // copy was unaffected
        assertEquals(b.getLong("paths/x/extra", 0), 0);
        assertEquals(b.getListNN("paths/x/values").size(), 2);
    }

    @Test
    public void testIdPathOnObjectOverlay() throws IOException {
        DictTree base = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    name: a\n" +
                "    value: 1\n");
        DictTree matching = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    name: a\n" +
                "    extra: 3\n");
        DictTree nonMatching = DictTree.load().fromYamlString("" +
                "paths:\n" +
                "  x:\n" +
                "    name: b\n" +
                "    extra: 3\n");
        for (String idPath : asList("x/name", "paths/x/name")) {
            DictTree result = DictTree.overlay(base, matching, idPath);
            assertEquals(result.getLong("paths/x/value", 0), 1);
            assertEquals(result.getLong("paths/x/extra", 0), 3);
            assertEquals(result.getListNN("paths/x").size(), 1);

            result = DictTree.overlay(base, nonMatching, idPath);
            assertEquals(result.getLong("paths/x/value", 0), 1);
            assertEquals(result.getLong("paths/x/extra", 0), 0);
            assertEquals(result.getListNN("paths/x").size(), 1);
        }

        // if no idPath, always overlay
        DictTree result = DictTree.overlay(base, nonMatching);
        assertEquals(result.getLong("paths/x/value", 0), 1);
        assertEquals(result.getLong("paths/x/extra", 0), 3);
        assertEquals(result.getListNN("paths/x").size(), 1);
    }

    @Test
    public void testReplaceValue() throws IOException {
        DictTree dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        Object old = dict.put("x", "c");
        assertTrue(old instanceof DictTree);
        assertEquals(((DictTree)old).keySet(), newHashSet("y", "z"));
        assertEquals(dict.get("x"), "c");
        assertNull(dict.get("x/y"));

        dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        assertEquals(dict.getMapNN("x").put("y", "c"), "a");
        assertEquals(dict.get("x/y"), "c");
        assertEquals(dict.get("x/z"), singletonList("b"));

        dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        assertEquals(dict.getMapNN("x").put("z", "c"), singletonList("b"));
        assertEquals(dict.get("x/y"), "a");
        assertEquals(dict.get("x/z"), "c");
    }

    @Test
    public void testRemoveValue() throws IOException {
        DictTree dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        Object old = dict.remove("x");
        assertTrue(old instanceof DictTree);
        assertEquals(((DictTree)old).keySet(), newHashSet("y", "z"));
        assertTrue(dict.isEmpty());

        dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        assertEquals(dict.getMapNN("x").remove("y"), "a");
        assertNull(dict.get("x/y"));
        assertEquals(dict.get("x/z"), singletonList("b"));

        dict = DictTree.load().fromJsonString("{\"x\": {\"y\": \"a\", \"z\": [\"b\"]}}");
        assertEquals(dict.getMapNN("x").remove("z"), singletonList("b"));
        assertEquals(dict.get("x/y"), "a");
        assertNull(dict.get("x/z"));
    }

    @Test
    public void testOverlayRef() throws IOException {
        DictTree tree = DictTree.load().fromResource(getClass(), "overlay-ref.json");
        assertEquals(tree.getString("definitions/def_a/prop_a"), "value_a");
        assertEquals(tree.getLong("child/ref/prop_d"), 5); //added property
        assertNull(tree.getString("child/ref/prop_a")); // removed property
        assertEquals(tree.getString("definitions/def_a/prop_a"), "value_a"); // no side effect
        assertNull(tree.getString("child/ref_2/prop_a")); // no side effect

        assertEquals(tree.getDouble("child/ref/sub_ref/x"), 23.5); //deref after overlay
        DictTree sub = tree.getMapNN("child/ref/sub_ref");
        assertEquals(sub.getDouble("subsub_ref/y"), 52.3);
    }
}