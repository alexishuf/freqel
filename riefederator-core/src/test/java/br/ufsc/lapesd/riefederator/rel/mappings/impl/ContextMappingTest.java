package br.ufsc.lapesd.riefederator.rel.mappings.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.DictTree;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class ContextMappingTest implements TestContext {

    private @Nonnull ContextMapping parse(@Nonnull String contextOrResource,
                                          @Nullable String table)
            throws IOException, ContextMappingParseException {
        DictTree d;
        if (contextOrResource.isEmpty() || contextOrResource.startsWith("{"))
            d = DictTree.load().fromJsonString(contextOrResource);
        else
            d = DictTree.load().fromResource(ContextMappingTest.class, contextOrResource);
        return ContextMapping.parse(d, table);
    }

    @DataProvider()
    public static Object[][] parseMoleculeData() {
        return Stream.of(
                asList("{}", "Table", Molecule.builder("Table")
                        .tag(new TableTag("Table")).exclusive().build()),
                asList("mapping-1.json", null, Molecule.builder("T1")
                        .out(type, new Atom(RDF.type.getURI()))
                        .out(name, new Atom("name"), new ColumnTag("T1", "name"))
                        .tag(new TableTag("T1")).exclusive().build()),
                asList("mapping-2.json", null, Molecule.builder("T2")
                        .out(type, new Atom(RDF.type.getURI()))
                        .out(age, new Atom("age"), new ColumnTag("T2", "age"))
                        .tag(new TableTag("T2")).exclusive().build()),
                asList("mapping-3.json", null, Molecule.builder("T3")
                        .out(type, new Atom(RDF.type.getURI()))
                        .out(age, new Atom("age"), new ColumnTag("T3", "age"))
                        .tag(new TableTag("T3")).exclusive().build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseMoleculeData")
    public void testParseMolecule(@Nonnull String contextOrResource, @Nullable String tableName,
                                  @Nonnull Molecule molecule) throws Exception {
        ContextMapping mapping = parse(contextOrResource, tableName);
        assertEquals(mapping.createMolecule(), molecule);
        assertEquals(mapping.createMolecule((Map<String, List<String>>) null), molecule);
        assertEquals(mapping.createMolecule((Collection<Column>)null), molecule);
    }

    @DataProvider
    public static Object[][] createSubMoleculeData() {
        return Stream.of(
                asList("mapping-1.json", "T1", singletonList("name"),
                        Molecule.builder("T1")
                                .out(type, new Atom(type.getURI()))
                                .out(name, new Atom("name"), new ColumnTag("T1", "name"))
                                .tag(new TableTag("T1")).exclusive().build()),
                asList("mapping-1.json", "T1", asList("name", "age"),
                        Molecule.builder("T1")
                                .out(type, new Atom(type.getURI()))
                                .out(name, new Atom("name"), new ColumnTag("T1", "name"))
                                .out(age, new Atom("age"), new ColumnTag("T1", "age"))
                                .tag(new TableTag("T1")).exclusive().build()),
                asList("mapping-2.json", "T2", singletonList("age"),
                        Molecule.builder("T2")
                                .out(type, new Atom(type.getURI()))
                                .out(age, new Atom("age"), new ColumnTag("T2", "age"))
                                .tag(new TableTag("T2")).exclusive().build()),
                asList("mapping-3.json", "T3", singletonList("age"),
                        Molecule.builder("T3")
                                .out(type, new Atom(type.getURI()))
                                .out(age, new Atom("age"), new ColumnTag("T3", "age"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("mapping-3.json", "T3", singletonList("name"),
                        Molecule.builder("T3")
                                .out(type, new Atom(type.getURI()))
                                .out(new StdPlain("name"), new Atom("name"),
                                        new ColumnTag("T3", "name"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("{}", "T", singletonList("name"),
                        Molecule.builder("T")
                                .out(new StdPlain("name"), new Atom("name"),
                                        new ColumnTag("T", "name"))
                                .tag(new TableTag("T")).exclusive().build()),
                asList("{}", "T", asList("name", "age"),
                        Molecule.builder("T")
                                .out(new StdPlain("name"), new Atom("name"),
                                        new ColumnTag("T", "name"))
                                .out(new StdPlain("age"), new Atom("age"),
                                        new ColumnTag("T", "age"))
                                .tag(new TableTag("T")).exclusive().build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "createSubMoleculeData")
    public void testCreateSubMolecule(@Nonnull String contextOrResource, @Nonnull String table,
                                      @Nonnull List<String> columns,
                                      @Nonnull Molecule molecule) throws Exception {
        ContextMapping mapping = parse(contextOrResource, table);
        Map<String, List<String>> map = new HashMap<>();
        map.put(table, columns);
        assertEquals(mapping.createMolecule(map), molecule);

        Molecule actual = mapping.createMolecule(columns.stream().map(c -> new Column(table, c))
                                                        .collect(toList()));
        assertEquals(actual, molecule);
    }

    @DataProvider
    public static Object[][] toRDFData() {
        return Stream.of(
                asList("mapping-1.json", "T1", singletonList("name"), singletonList("bob"),
                       "to-rdf-1.ttl"),
                asList("mapping-1.json", "T1", asList("name", "age"), asList("bob", 22),
                        "to-rdf-2.ttl"),
                asList("{}", "T", singletonList("name"), singletonList("bob"),
                       "to-rdf-3.ttl"),
                asList("mapping-2.json", "T2", asList("name", "age"), asList("bob", 22),
                       "to-rdf-4.ttl"),
                asList("mapping-3.json", "T3", singletonList("age"), singletonList(23),
                        "to-rdf-5.ttl")
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
        ContextMapping mapping = parse(contextOrResource, table);
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