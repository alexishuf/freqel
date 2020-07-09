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
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
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
                        .tag(new TableTag("Table")).exclusive().build(),
                        emptySet()),
                asList("mapping-1.json", null, Molecule.builder("T1")
                        .out(type, new Atom("T1."+RDF.type.getURI()))
                        .out(name, Molecule.builder("T1.name").tag(new ColumnTag("T1", "name"))
                                           .buildAtom(),
                                new ColumnTag("T1", "name"))
                        .tag(new TableTag("T1")).exclusive().build(),
                        singleton("name")),
                asList("mapping-2.json", null, Molecule.builder("T2")
                        .out(type, new Atom("T2."+RDF.type.getURI()))
                        .out(age, Molecule.builder("T2.age").tag(new ColumnTag("T2", "age"))
                                          .buildAtom(),
                                new ColumnTag("T2", "age"))
                        .tag(new TableTag("T2")).exclusive().build(),
                        emptySet()),
                asList("mapping-3.json", null, Molecule.builder("T3")
                        .out(type, new Atom("T3."+RDF.type.getURI()))
                        .out(age, Molecule.builder("T3.age").tag(new ColumnTag("T3", "age"))
                                          .buildAtom(),
                                new ColumnTag("T3", "age"))
                        .tag(new TableTag("T3")).exclusive().build(),
                        emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseMoleculeData")
    public void testParseMolecule(@Nonnull String contextOrResource, @Nullable String tableName,
                                  @Nonnull Molecule molecule,
                                  @Nonnull Set<String> idColumns) throws Exception {
        ContextMapping mapping = parse(contextOrResource, tableName);
        assertEquals(mapping.createMolecule(), molecule);
        assertEquals(mapping.createMolecule((Map<String, List<String>>) null), molecule);
        assertEquals(mapping.createMolecule((Collection<Column>)null), molecule);

        assertEquals(mapping.getIdColumns(), idColumns); //not interface
        String acTable = molecule.getCore().getTags().stream()
                .filter(TableTag.class::isInstance)
                .map(t -> ((TableTag) t).getTable()).findFirst().orElse(null);
        assertNotNull(acTable);
        assertEquals(mapping.getIdColumnsNames(acTable), idColumns);
        assertEquals(mapping.getIdColumnsNames(acTable, idColumns), idColumns);
        assertEquals(mapping.getIdColumnsNames(acTable, singletonList("name")), idColumns);
        assertEquals(mapping.getIdColumnsNames(acTable,
                                               singletonList(new Column(acTable, "name"))),
                     idColumns);
        assertEquals(mapping.getIdColumns(acTable),
                     idColumns.stream().map(n -> new Column(acTable, n)).collect(toSet()));
        assertEquals(mapping.getIdColumns(acTable, singletonList("name")),
                     idColumns.stream().map(n -> new Column(acTable, n)).collect(toSet()));
        assertEquals(mapping.getIdColumns(acTable, singletonList(new Column(acTable, "name"))),
                idColumns.stream().map(n -> new Column(acTable, n)).collect(toSet()));
    }

    @DataProvider
    public static Object[][] createSubMoleculeData() {
        return Stream.of(
                asList("mapping-1.json", "T1", singletonList("name"),
                        Molecule.builder("T1")
                                .out(type, new Atom("T1."+type.getURI()))
                                .out(name, Molecule.builder("T1.name")
                                                   .tag(new ColumnTag("T1", "name")).buildAtom(),
                                        new ColumnTag("T1", "name"))
                                .tag(new TableTag("T1")).exclusive().build()),
                asList("mapping-1.json", "T1", asList("name", "age"),
                        Molecule.builder("T1")
                                .out(type, new Atom("T1."+type.getURI()))
                                .out(name, Molecule.builder("T1.name")
                                                   .tag(new ColumnTag("T1", "name")).buildAtom(),
                                        new ColumnTag("T1", "name"))
                                .out(age, Molecule.builder("T1.age")
                                                   .tag(new ColumnTag("T1", "age")).buildAtom(),
                                        new ColumnTag("T1", "age"))
                                .tag(new TableTag("T1")).exclusive().build()),
                asList("mapping-2.json", "T2", singletonList("age"),
                        Molecule.builder("T2")
                                .out(type, new Atom("T2."+type.getURI()))
                                .out(age, Molecule.builder("T2.age")
                                                  .tag(new ColumnTag("T2", "age")).buildAtom(),
                                        new ColumnTag("T2", "age"))
                                .tag(new TableTag("T2")).exclusive().build()),
                asList("mapping-3.json", "T3", singletonList("age"),
                        Molecule.builder("T3")
                                .out(type, new Atom("T3."+type.getURI()))
                                .out(age, Molecule.builder("T3.age")
                                                  .tag(new ColumnTag("T3", "age")).buildAtom(),
                                        new ColumnTag("T3", "age"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("mapping-3.json", "T3", singletonList("name"),
                        Molecule.builder("T3")
                                .out(type, new Atom("T3."+type.getURI()))
                                .out(new StdPlain("name"),
                                        Molecule.builder("T3.name")
                                                .tag(new ColumnTag("T3", "name")).buildAtom(),
                                        new ColumnTag("T3", "name"))
                                .tag(new TableTag("T3")).exclusive().build()),
                asList("{}", "T", singletonList("name"),
                        Molecule.builder("T")
                                .out(new StdPlain("name"),
                                        Molecule.builder("T.name").tag(new ColumnTag("T", "name"))
                                                .buildAtom(),
                                        new ColumnTag("T", "name"))
                                .tag(new TableTag("T")).exclusive().build()),
                asList("{}", "T", asList("name", "age"),
                        Molecule.builder("T")
                                .out(new StdPlain("name"),
                                        Molecule.builder("T.name").tag(new ColumnTag("T", "name"))
                                                .buildAtom(),
                                        new ColumnTag("T", "name"))
                                .out(new StdPlain("age"),
                                        Molecule.builder("T.age").tag(new ColumnTag("T", "age"))
                                                .buildAtom(),
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