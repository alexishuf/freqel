package br.ufsc.lapesd.freqel.rel.mappings.context;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.tags.ValueTag;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.PostRelationalTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.util.DictTree;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ContextMappingTest implements TestContext {

    private @Nonnull ContextMapping parse(@Nonnull String contextOrResource)
            throws IOException, ContextMappingParseException {
        List<DictTree> list;
        if (contextOrResource.isEmpty() || contextOrResource.startsWith("{"))
            list = DictTree.load().fromJsonStringList(contextOrResource);
        else
            list = DictTree.load().fromResourceList(ContextMappingTest.class, contextOrResource);
        return ContextMapping.parse(list);
    }

    @DataProvider()
    public static Object[][] parseMoleculeData() {
        return Stream.of(
                asList("{\"@tableName\": \"Table\"}", Molecule.builder("Table")
                        .tag(new TableTag("Table")).exclusive().build(),
                        "{\"Table\":[]}"),
                asList("mapping-1.json", Molecule.builder("T1")
                        .out(type, Molecule.builder("T1."+RDF.type.getURI())
                                        .tag(PostRelationalTag.INSTANCE)
                                        .tag(ValueTag.of(Person))
                                        .buildAtom(),
                                PostRelationalTag.INSTANCE)
                        .out(name, Molecule.builder("T1.name").tag(ColumnsTag.direct("T1", "name"))
                                           .buildAtom(),
                                ColumnsTag.direct("T1", "name"))
                        .tag(new TableTag("T1"))
                        .tag(ColumnsTag.nonDirect("T1", "name"))
                        .exclusive().build(),
                        "{\"T1\": [\"name\"]}"),
                asList("mapping-2.json", Molecule.builder("T2")
                        .out(type, Molecule.builder("T2."+RDF.type.getURI())
                                        .tag(PostRelationalTag.INSTANCE)
                                        .tag(ValueTag.of(Person))
                                        .tag(ValueTag.of(Document))
                                        .buildAtom(),
                                PostRelationalTag.INSTANCE)
                        .out(age, Molecule.builder("T2.age").tag(ColumnsTag.direct("T2", "age"))
                                          .buildAtom(),
                                ColumnsTag.direct("T2", "age"))
                        .tag(new TableTag("T2")).exclusive().build(),
                        "{\"T2\":[]}"),
                asList("mapping-3.json", Molecule.builder("T3")
                        .out(type, Molecule.builder("T3."+RDF.type.getURI())
                                        .tag(PostRelationalTag.INSTANCE)
                                        .tag(ValueTag.of(Person)).buildAtom(),
                                PostRelationalTag.INSTANCE)
                        .out(age, Molecule.builder("T3.age").tag(ColumnsTag.direct("T3", "age"))
                                          .buildAtom(),
                                ColumnsTag.direct("T3", "age"))
                        .tag(new TableTag("T3")).exclusive().build(),
                        "{\"T3\":[]}"),
                asList("mapping-4.json",
                       Molecule.builder("T1")
                            .out(type, Molecule.builder("T1."+RDF.type.getURI())
                                            .tag(PostRelationalTag.INSTANCE)
                                            .tag(ValueTag.of(Person))
                                            .buildAtom(),
                                    PostRelationalTag.INSTANCE)
                            .out(name, Molecule.builder("T1.name").tag(ColumnsTag.direct("T1", "name"))
                                            .buildAtom(),
                                    ColumnsTag.direct("T1", "name"))
                            .tag(new TableTag("T1"))
                           .tag(ColumnsTag.nonDirect("T1", "name"))
                           .exclusive()
                       .startNewCore("T3")
                            .out(type, Molecule.builder("T3."+RDF.type.getURI())
                                            .tag(PostRelationalTag.INSTANCE)
                                            .tag(ValueTag.of(Person))
                                            .buildAtom(),
                                    PostRelationalTag.INSTANCE)
                            .out(age, Molecule.builder("T3.age").tag(ColumnsTag.direct("T3", "age"))
                                            .buildAtom(),
                                    ColumnsTag.direct("T3", "age"))
                            .tag(new TableTag("T3")).exclusive()
                       .startNewCore("T4")
                               .out(type, Molecule.builder("T4."+RDF.type.getURI())
                                               .tag(PostRelationalTag.INSTANCE)
                                               .tag(ValueTag.of(Professor))
                                               .buildAtom(),
                                       PostRelationalTag.INSTANCE)
                               .out(name, Molecule.builder("T4.name")
                                               .tag(ColumnsTag.direct("T4", "name")).buildAtom(),
                                       ColumnsTag.direct("T4", "name"))
                               .tag(new TableTag("T4")).exclusive().build(),
                       "{\"T1\": [\"name\"], \"T3\":[], \"T4\":[]}"),
                asList("mapping-5.json",
                        Molecule.builder("T5")
                                .out(type, Molecule.builder("T5."+RDF.type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person))
                                                .buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(name, Molecule.builder("T5.name")
                                        .tag(ColumnsTag.direct("T5", "name")).buildAtom(),
                                        ColumnsTag.direct("T5", "name"))
                                .out(age, Molecule.builder("T5.age")
                                        .tag(ColumnsTag.direct("T5", "age")).buildAtom(),
                                        ColumnsTag.direct("T5", "age"))
                                .tag(new TableTag("T5"))
                                .tag(new ColumnsTag(asList(new Column("T5", "name"),
                                                                 new Column("T5", "age"))))
                                .exclusive().build(),
                        "{\"T5\": [\"name\", \"age\"]}")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseMoleculeData")
    public void testParseMolecule(@Nonnull String contextOrResource,
                                  @Nonnull Molecule molecule,
                                  @Nonnull String idColumnsJson) throws Exception {
        DictTree idColumns = DictTree.load().fromJsonString(idColumnsJson);
        ContextMapping mapping = parse(contextOrResource);
        assertEquals(mapping.createMolecule(), molecule);
        assertEquals(mapping.createMolecule((Map<String, List<String>>) null), molecule);
        assertEquals(mapping.createMolecule((Collection<Column>)null), molecule);

        for (String table : idColumns.keySet()) {
            Set<String> set = idColumns.getListNN(table).stream().map(Object::toString)
                                                                 .collect(toSet());
            assertEquals(new HashSet<>(mapping.getIdColumnsNames(table)), set);
            assertEquals(new HashSet<>(mapping.getIdColumnsNames(table, set)), set);
            assertEquals(new HashSet<>(mapping.getIdColumnsNames(table, singletonList("name"))), set);
            assertEquals(new HashSet<>(mapping.getIdColumnsNames(table,
                    singletonList(new Column(table, "name")))),
                    set);
            assertEquals(new HashSet<>(mapping.getIdColumns(table)),
                    set.stream().map(n -> new Column(table, n)).collect(toSet()));
            assertEquals(new HashSet<>(mapping.getIdColumns(table, singletonList("name"))),
                    set.stream().map(n -> new Column(table, n)).collect(toSet()));
            assertEquals(new HashSet<>(mapping.getIdColumns(table, singletonList(new Column(table, "name")))),
                    set.stream().map(n -> new Column(table, n)).collect(toSet()));
        }

    }

    @DataProvider
    public static Object[][] createSubMoleculeData() {
        return Stream.of(
                asList("mapping-1.json", "{\"T1\": [\"name\"]}",
                        Molecule.builder("T1")
                                .out(type, Molecule.builder("T1."+type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(name, Molecule.builder("T1.name")
                                                   .tag(ColumnsTag.direct("T1", "name")).buildAtom(),
                                        ColumnsTag.direct("T1", "name"))
                                .tag(new TableTag("T1"))
                                .tag(ColumnsTag.nonDirect("T1", "name"))
                                .exclusive().build()),
                asList("mapping-1.json", "{\"T1\": [\"name\", \"age\"]}",
                        Molecule.builder("T1")
                                .out(type, Molecule.builder("T1."+type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(name, Molecule.builder("T1.name")
                                                   .tag(ColumnsTag.direct("T1", "name")).buildAtom(),
                                        ColumnsTag.direct("T1", "name"))
                                .out(age, Molecule.builder("T1.age")
                                                   .tag(ColumnsTag.direct("T1", "age")).buildAtom(),
                                        ColumnsTag.direct("T1", "age"))
                                .tag(new TableTag("T1"))
                                .tag(ColumnsTag.nonDirect("T1", "name"))
                                .exclusive().build()),
                asList("mapping-2.json", "{\"T2\": [\"age\"]}",
                        Molecule.builder("T2")
                                .out(type, Molecule.builder("T2."+type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person))
                                                .tag(ValueTag.of(Document)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(age, Molecule.builder("T2.age")
                                                  .tag(ColumnsTag.direct("T2", "age")).buildAtom(),
                                        ColumnsTag.direct("T2", "age"))
                                .tag(new TableTag("T2")).exclusive().build()),
                asList("mapping-3.json", "{\"T3\": [\"age\"]}",
                        Molecule.builder("T3")
                                .out(type, Molecule.builder("T3."+type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(age, Molecule.builder("T3.age")
                                                  .tag(ColumnsTag.direct("T3", "age")).buildAtom(),
                                        ColumnsTag.direct("T3", "age"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("mapping-3.json", "{\"T3\": [\"name\"]}",
                        Molecule.builder("T3")
                                .out(type, Molecule.builder("T3." + type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(new StdPlain("name"),
                                        Molecule.builder("T3.name")
                                                .tag(ColumnsTag.direct("T3", "name")).buildAtom(),
                                        ColumnsTag.direct("T3", "name"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("{\"@tableName\": \"T\"}", "{\"T\": [\"name\"]}",
                        Molecule.builder("T")
                                .out(new StdPlain("name"),
                                        Molecule.builder("T.name").tag(ColumnsTag.direct("T", "name"))
                                                .buildAtom(),
                                        ColumnsTag.direct("T", "name"))
                                .tag(new TableTag("T")).exclusive().build()),
                asList("{\"@tableName\": \"T\"}", "{\"T\": [\"name\", \"age\"]}",
                        Molecule.builder("T")
                                .out(new StdPlain("name"),
                                        Molecule.builder("T.name").tag(ColumnsTag.direct("T", "name"))
                                                .buildAtom(),
                                        ColumnsTag.direct("T", "name"))
                                .out(new StdPlain("age"),
                                        Molecule.builder("T.age").tag(ColumnsTag.direct("T", "age"))
                                                .buildAtom(),
                                        ColumnsTag.direct("T", "age"))
                                .tag(new TableTag("T")).exclusive().build()),
                asList("mapping-4.json", "{\"T1\": [\"name\", \"age\"]}",
                        Molecule.builder("T1")
                                .out(type, Molecule.builder("T1."+RDF.type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(name, Molecule.builder("T1.name").tag(ColumnsTag.direct("T1", "name"))
                                                .buildAtom(),
                                        ColumnsTag.direct("T1", "name"))
                                .out(age, Molecule.builder("T1.age").tag(ColumnsTag.direct("T1", "age"))
                                                .buildAtom(),
                                        ColumnsTag.direct("T1", "age"))
                                .tag(new TableTag("T1"))
                                .tag(ColumnsTag.nonDirect("T1", "name"))
                                .exclusive().build()),
                asList("mapping-4.json", "{\"T3\": [\"name\"], \"T4\": [\"name\", \"age\"]}",
                        Molecule.builder("T3")
                                .out(type, Molecule.builder("T3."+RDF.type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Person)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(new StdPlain("name"), Molecule.builder("T3.name")
                                                .tag(ColumnsTag.direct("T3", "name")).buildAtom(),
                                        ColumnsTag.direct("T3", "name"))
                                .tag(new TableTag("T3")).exclusive()
                                .startNewCore("T4")
                                .out(type, Molecule.builder("T4."+RDF.type.getURI())
                                                .tag(PostRelationalTag.INSTANCE)
                                                .tag(ValueTag.of(Professor)).buildAtom(),
                                        PostRelationalTag.INSTANCE)
                                .out(name, Molecule.builder("T4.name")
                                                .tag(ColumnsTag.direct("T4", "name")).buildAtom(),
                                        ColumnsTag.direct("T4", "name"))
                                .out(new StdPlain("age"), Molecule.builder("T4.age")
                                                .tag(ColumnsTag.direct("T4", "age")).buildAtom(),
                                        ColumnsTag.direct("T4", "age"))
                                .tag(new TableTag("T4")).exclusive().build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "createSubMoleculeData")
    public void testCreateSubMolecule(@Nonnull String contextOrResource,
                                      @Nonnull String columnsJson,
                                      @Nonnull Molecule molecule) throws Exception {
        ContextMapping mapping = parse(contextOrResource);
        DictTree columns = DictTree.load().fromJsonString(columnsJson);
        Map<String, List<String>> map = new HashMap<>();
        for (String table : columns.keySet()) {
            map.put(table, columns.getListNN(table).stream().map(Object::toString)
                                                            .collect(toList()));
        }
        assertEquals(mapping.createMolecule(map), molecule);

        List<Column> columnsList = map.entrySet().stream().flatMap(e -> e.getValue().stream()
                .map(c -> new Column(e.getKey(), c))).collect(toList());
        Molecule actual = mapping.createMolecule(columnsList);
        assertEquals(actual, molecule);
    }

    @DataProvider
    public static Object[][] toRDFData() {
        return Stream.of(
                asList("mapping-1.json", "T1", singletonList("name"), singletonList("bob"),
                       "to-rdf-1.ttl"),
                asList("mapping-1.json", "T1", asList("name", "age"), asList("bob", 22),
                        "to-rdf-2.ttl"),
                asList("{\"@tableName\": \"T\"}", "T", singletonList("name"), singletonList("bob"),
                       "to-rdf-3.ttl"),
                asList("mapping-2.json", "T2", asList("name", "age"), asList("bob", 22),
                       "to-rdf-4.ttl"),
                asList("mapping-3.json", "T3", singletonList("age"), singletonList(23),
                        "to-rdf-5.ttl"),
                asList("mapping-4.json", "T1", asList("name", "age"), asList("bob", 22),
                        "to-rdf-6.ttl"),
                asList("mapping-4.json", "T4", asList("name", "age"), asList("alice", 23),
                        "to-rdf-7.ttl")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "toRDFData")
    public void testToRDF(@Nonnull String contextOrResource, @Nonnull String table,
                          @Nonnull List<String> columns, @Nonnull List<Object> values,
                          @Nonnull String expectedResource) throws Exception {
        ContextMapping.resetNextIdForTesting();
        Model expected = ModelFactory.createDefaultModel();
        try (InputStream in = getClass().getResourceAsStream(expectedResource)) {
            assertNotNull(in);
            RDFDataMgr.read(expected, in, Lang.TTL);
        }
        ContextMapping mapping = parse(contextOrResource);
        Model actual = ModelFactory.createDefaultModel();

        Term nameFor = mapping.getNameFor(table, columns, values);
        mapping.toRDF(actual, table, columns, values);
        assertTrue(actual.isIsomorphicWith(expected));
        if (!nameFor.isBlank())
            assertTrue(actual.containsResource(JenaWrappers.toJena(nameFor)));

        ContextMapping.resetNextIdForTesting();
        actual.removeAll();
        List<Column> colObjects = columns.stream().map(s -> new Column(table, s)).collect(toList());
        nameFor = mapping.getNameFor(colObjects, values);
        mapping.toRDF(actual, colObjects, values);
        assertTrue(actual.isIsomorphicWith(expected));
        if (!nameFor.isBlank())
            assertTrue(actual.containsResource(JenaWrappers.toJena(nameFor)));

        ContextMapping.resetNextIdForTesting();
        actual.removeAll();
        Map<Column, Object> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            map.put(new Column(table, columns.get(i)), values.get(i));
        nameFor = mapping.getNameFor(map);
        mapping.toRDF(actual, map);
        assertTrue(actual.isIsomorphicWith(expected));
        if (!nameFor.isBlank())
            assertTrue(actual.containsResource(JenaWrappers.toJena(nameFor)));

        ContextMapping.resetNextIdForTesting();
        actual.removeAll();
        Map<String, Object> map2 = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            map2.put(columns.get(i), values.get(i));
        nameFor = mapping.getNameFor(table, map2);
        mapping.toRDF(actual, table, map2);
        assertTrue(actual.isIsomorphicWith(expected));
        if (!nameFor.isBlank())
            assertTrue(actual.containsResource(JenaWrappers.toJena(nameFor)));
    }
}