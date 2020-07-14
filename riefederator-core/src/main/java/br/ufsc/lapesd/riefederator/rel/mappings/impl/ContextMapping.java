package br.ufsc.lapesd.riefederator.rel.mappings.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;

@Immutable
public class ContextMapping implements RelationalMapping {
    private static final Logger logger = LoggerFactory.getLogger(ContextMapping.class);
    private static final @Nonnull AtomicLong nextId = new AtomicLong(0);

    public enum UriGeneratorType {
        CONCAT,
        SEQ,
        BLANK
    }

    private final @Nonnull Map<String, TableContext> table2context;
    private @LazyInit Molecule fullMolecule = null;

    @VisibleForTesting
    static void resetNextIdForTesting() {
        nextId.set(0);
    }

    public ContextMapping(@Nonnull Map<String, TableContext> contexts) {
        this.table2context = contexts;
    }

    /* --- --- --- builder --- --- --- */

    public static class TableBuilder {
        private @Nonnull final Builder parent;
        private @Nonnull final String tableName;
        private @Nonnull final Map<String, URI> column2uri = new HashMap<>();
        private @Nullable String fallbackPrefix = StdPlain.URI_PREFIX;
        private @Nonnull final Set<URI> classes = new HashSet<>();
        private @Nullable String instancePrefix = StdPlain.URI_PREFIX;
        private @Nonnull final List<String> idColumns = new ArrayList<>();
        private @Nonnull String idColumnsSeparator = "-";
        private boolean exclusive = true;
        private @Nullable UriGeneratorType uriGeneratorType = null;

        public TableBuilder(@Nonnull Builder parent, @Nonnull String tableName) {
            this.parent = parent;
            this.tableName = tableName;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder exclusive(boolean value) {
            exclusive = value;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder column2uri(@Nonnull String column,
                                                                 @Nonnull URI uri) {
            this.column2uri.put(column, uri);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder fallbackPrefix(@Nullable String prefix) {
            this.fallbackPrefix = prefix;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder addClass(@Nonnull URI cls) {
            this.classes.add(cls);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder instancePrefix(@Nullable String prefix) {
            this.instancePrefix = prefix;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder addIdColumn(@Nonnull String column) {
            this.idColumns.add(column);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull TableBuilder idColumnsSeparator(@Nonnull String sep) {
            this.idColumnsSeparator = sep;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull
        TableBuilder uriGenerator(@Nonnull UriGeneratorType type) {
            this.uriGeneratorType = type;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder endTable() {
            if (uriGeneratorType == null) {
                if (!idColumns.isEmpty())
                    uriGeneratorType = UriGeneratorType.CONCAT;
                else
                    uriGeneratorType = UriGeneratorType.BLANK;
            }
            TableContext tableContext = new TableContext(tableName,
                    ImmutableMap.copyOf(column2uri), fallbackPrefix,
                    ImmutableSet.copyOf(classes), instancePrefix,
                    ImmutableList.copyOf(idColumns), idColumnsSeparator,
                    exclusive, uriGeneratorType);
            parent.add(tableContext);
            return parent;
        }
    }

    public static class Builder {
        private @Nonnull final Map<String, TableContext> table2context = new HashMap<>();

        public @Nonnull TableBuilder beginTable(@Nonnull String tableName) {
            Preconditions.checkState(!table2context.containsKey(tableName),
                                     "Table "+tableName+" already registered!");
            return new TableBuilder(this, tableName);
        }

        public @CanIgnoreReturnValue @Nonnull Builder add(@Nonnull TableContext context) {
            table2context.put(context.getTableName(), context);
            return this;
        }

        public @CheckReturnValue @Nonnull ContextMapping build() {
            Preconditions.checkState(!table2context.isEmpty(), "No Tables!");
            return new ContextMapping(table2context);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    /* --- --- --- parse --- --- --- */

    /**
     * See {@link ContextMapping#parse(DictTree)}
     */
    public static @Nonnull ContextMapping parse(@Nonnull InputStream stream)
            throws IOException, ContextMappingParseException {
        return parse(DictTree.load().fromInputStreamList(stream));
    }

    /**
     * See {@link ContextMapping#parse(DictTree)}
     */
    public static @Nonnull ContextMapping parse(@Nonnull Collection<DictTree> collection)
            throws ContextMappingParseException {
        Map<String, TableContext> table2context = new HashMap<>();
        for (DictTree dictTree : collection) {
            ContextMapping member = parse(dictTree);
            assert member.getTableNames().size() == 1;
            String table = member.getTableNames().iterator().next();
            if (table2context.containsKey(table))
                throw new IllegalArgumentException("Duplicate table "+table);
            table2context.putAll(member.table2context);
        }
        return new ContextMapping(table2context);
    }

    /**
     * Parses the mapping from the already parsed YAML/JSON/JSON-LD source.
     *
     * @param d the parsed {@link DictTree}
     * @return The functional {@link ContextMapping}
     * @throws ContextMappingParseException If something is wrong with syntax of the values in
     *                                      the file (beyond JSON/YAML syntax)
     */
    public static @Nonnull ContextMapping parse(@Nonnull DictTree d)
            throws ContextMappingParseException {
        String tableName = d.getString("@tableName", d.getString("tableName"));
        if (tableName == null)
            throw new ContextMappingParseException("No @tableName defined");
        TableBuilder b = builder().beginTable(tableName);

        String fallbackPrefix = getString(d, "fallbackPrefix", StdPlain.URI_PREFIX);
        b.fallbackPrefix(fallbackPrefix);
        b.instancePrefix(getString(d, "instancePrefix", null));
        b.idColumnsSeparator(d.getString("@uriPropertySeparator",
                                         d.getString("uriPropertySeparator", "-")));

        DictTree contextDict = d.getMapNN("@context");
        Map<String, Object> context = contextDict.asMap();
        for (Map.Entry<String, Object> e : context.entrySet()) {
            if (e.getKey().equals("@uriProperty")) {
                if ("@GenerateUri".equalsIgnoreCase(Objects.toString(e.getValue()))) {
                    b.uriGenerator(UriGeneratorType.SEQ);
                } else if ("@Blank".equalsIgnoreCase(Objects.toString(e.getValue()))) {
                    b.uriGenerator(UriGeneratorType.BLANK);
                } else if (e.getValue().toString().startsWith("@")) {
                    logger.warn("@uriProperty={} is not supported. Provide a column name " +
                            "or @GenerateUri", e.getValue());
                } else if (e.getValue() instanceof Collection) {
                    Collection<?> collection = (Collection<?>) e.getValue();
                    collection.forEach(o -> b.addIdColumn(Objects.toString(o)));
                } else {
                    b.addIdColumn(Objects.toString(e.getValue()));
                }
            } else if ("@type".equals(e.getKey())) {
                for (Object o : contextDict.getListNN("@type")) {
                    URI uri = toURI(o);
                    if (uri != null)
                        b.addClass(uri);
                }
            } else {
                URI uri = toURI(e.getValue());
                if (uri != null)
                    b.column2uri(e.getKey(), uri);
            }
        }
        return b.endTable().build();
    }

    /* --- --- --- internals --- --- --- */

    private static class ToRDFArgs {
        @Nonnull final List<Column> columns = new ArrayList<>();
        @Nonnull final List<Object> values = new ArrayList<>();
    }

    private static String getString(@Nonnull DictTree d, @Nonnull String key, String fallback) {
        String value = fallback;
        if (d.containsKey("@"+key))
            value = d.getString("@"+key);
        else if (d.containsKey(key))
            value = d.getString(key);
        return value;
    }

    private static @Nullable URI toURI(@Nullable Object possibleURI)
                                       throws BadUriContextMappingParsException {
        if (possibleURI instanceof Collection) {
            Collection<?> coll = (Collection<?>) possibleURI;
            if (coll.size() != 1) {
                logger.warn("Cannot consider collection {} as an URI or URI component", coll);
                return null;
            }
            possibleURI = coll.iterator().next();
        }
        if (possibleURI == null) return null;
        String uri = possibleURI.toString();
        try {
            new java.net.URI(uri);
        } catch (URISyntaxException e) {
            throw new BadUriContextMappingParsException(uri, e.getMessage());
        }
        return new StdURI(uri);
    }

    /* --- --- --- getters --- --- --- */

    public @Nonnull Set<String> getTableNames() {
        return table2context.keySet();
    }

    public @Nonnull TableContext getTableContext(@Nonnull String tableName) {
        TableContext context = table2context.getOrDefault(tableName, null);
        if (context == null)
            throw new NoSuchElementException("No table "+tableName+" knwon");
        return context;
    }

    /* --- --- --- interface implementation --- --- --- */

    @Override
    public @Nonnull Molecule createMolecule(@Nullable Map<String, List<String>> table2columns) {
        Set<String> tables;
        if      (table2columns != null) tables = table2columns.keySet();
        else if (fullMolecule  == null) tables =  table2context.keySet();
        else                            return fullMolecule;

        MoleculeBuilder b = null;
        for (String table : tables) {
            List<String> columns = table2columns == null ? null : table2columns.get(table);
            TableContext context = table2context.get(table);
            if (context == null) {
                assert false : "Unknown table "+table;
                logger.warn("Will ignore unknown table {} at createMolecule(). Expected one of {}",
                            table, table2context.keySet());
                continue;
            }
            b = context.addCore(b, columns);
        }

        if (b == null) {
            assert false : "Creating empty molecule";
            // if asserts are disabled, return an empty Molecule instead of blowing up
            logger.error("Creating empty molecule!");
            return Molecule.builder("").build();
        } else {
            Molecule m = b.build();
            if (table2columns == null)
                fullMolecule = m;
            return m;
        }
    }

    @Override
    public @Nonnull List<String> getIdColumnsNames(@Nonnull String table,
                                                  @Nullable Collection<?> columns) {
        // linked resources not supported (yet), thus columns is ignored
        TableContext context = table2context.get(table);
        if (context == null) {
            logger.warn("Bad name: {}. Expected one of {}", table, table2context.keySet());
            assert false : "Bad table name"+table; // abort if asserts enabled.
            return emptyList(); // else: try to continue
        }
        return context.getIdColumns();
    }

    @Override
    public @Nullable Term column2predicate(@Nonnull Column column) {
        TableContext context = table2context.get(column.getTable());
        if (context == null)
            return null;
        return context.getUri(column.getColumn());
    }

    @Override
    public @Nonnull Term getNameFor(@Nonnull List<Column> columns, @Nonnull List<?> values) {
        Preconditions.checkArgument(columns.size() == values.size(), "#columns != #values");
        if (columns.isEmpty()) {
            assert false : "Empty columns";
            logger.warn("No columns given! Will create a blank node");
            return JenaWrappers.fromJena(ResourceFactory.createResource());
        }
        String table = null;
        for (Column column : columns) {
            if (table == null) {
                table = column.getTable();
            } else if (!table.equals(column.getTable())) {
                assert false : "Columns have many tables, cannot determine which one to use";
                logger.warn("Creating a bank node since columns={} span multiple tables", columns);
                return JenaWrappers.fromJena(ResourceFactory.createResource());
            }
        }
        TableContext context = table2context.get(table);
        if (context == null) {
            assert false : "Unknown table";
            logger.warn("Table {} is not known. Expected one of {}. Will return a blank node",
                    table, table2context.keySet());
            return JenaWrappers.fromJena(ResourceFactory.createResource());
        }
        return JenaWrappers.fromJena(context.createResource(nextId, null, columns, values));
    }

    @Override
    public int toRDF(@Nonnull Model model, @Nonnull List<Column> columns, @Nonnull List<?> values) {
        Map<String, ToRDFArgs> table2args = new HashMap<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column col = columns.get(i);
            ToRDFArgs args = table2args.computeIfAbsent(col.getTable(), k -> new ToRDFArgs());
            args.columns.add(col);
            args.values.add(values.get(i));
        }
        int triples = 0;
        for (Map.Entry<String, ToRDFArgs> e : table2args.entrySet()) {
            TableContext context = table2context.get(e.getKey());
            if (context == null) {
                assert false : "Unexpected table";
                // if asserts are disable, only warn and ignore
                logger.warn("Ignoring unexpected table {}. Known tables: {}",
                            e.getKey(), table2context.keySet());
            } else {
                triples += context.toRDF(nextId, model, e.getValue().columns, e.getValue().values);
            }
        }
        return triples;
    }

    private @Nonnull Resource createResource(@Nullable Model model, @Nonnull List<Column> columns,
                                             @Nonnull List<?> values) {
        Preconditions.checkArgument(columns.size() == values.size(), "#columns != #values");
        if (columns.isEmpty()) {
            assert false : "Empty columns";
            logger.warn("No columns given! Will create a blank node");
            return model == null ? ResourceFactory.createResource() : model.createResource();
        }
        String table = null;
        for (Column column : columns) {
            if (table == null) {
                table = column.getTable();
            } else if (!table.equals(column.getTable())) {
                assert false : "Columns have many tables, cannot determine which one to use";
                logger.warn("Creating a bank node since columns={} span multiple tables", columns);
                return model == null ? ResourceFactory.createResource() : model.createResource();
            }
        }
        TableContext context = table2context.get(table);
        if (context == null) {
            assert false : "Unknown table";
            logger.warn("Table {} is not known. Expected one of {}. Will return a blank node",
                        table, table2context.keySet());
            return model == null ? ResourceFactory.createResource() : model.createResource();
        }
        return context.createResource(nextId, model, columns, values);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ContextMapping)) return false;
        ContextMapping that = (ContextMapping) o;
        return table2context.equals(that.table2context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table2context);
    }

    @Override
    public String toString() {
        return "ContextMapping"+getTableNames();
    }
}
