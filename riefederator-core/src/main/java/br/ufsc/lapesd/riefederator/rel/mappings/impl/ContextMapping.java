package br.ufsc.lapesd.riefederator.rel.mappings.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

@Immutable
public class ContextMapping implements RelationalMapping {
    private static final Logger logger = LoggerFactory.getLogger(ContextMapping.class);
    private static final URI rdfType = new StdURI(RDF.type.getURI());
    private static final @Nonnull AtomicLong nextId = new AtomicLong(0);

    public enum UriGeneratorType {
        CONCAT,
        SEQ,
        BLANK
    }

    private @Nonnull final String tableName;
    private @Nonnull final ImmutableMap<String, URI> column2uri;
    private @Nullable final String fallbackPrefix;
    private @Nonnull final ImmutableSet<URI> classes;
    private @Nullable final String instancePrefix;
    private @Nonnull final ImmutableSet<String> idColumns;
    private @Nonnull final String idColumnsSeparator;
    private final boolean exclusive;
    private final @Nonnull UriGeneratorType uriGeneratorType;
    private @LazyInit Molecule fullMolecule = null;

    @VisibleForTesting
    static void resetNextIdForTesting() {
        nextId.set(0);
    }

    public ContextMapping(@Nonnull String tableName,
                          @Nonnull ImmutableMap<String, URI> column2uri,
                          @Nullable String fallbackPrefix, @Nonnull ImmutableSet<URI> classes,
                          @Nullable String instancePrefix,
                          @Nonnull ImmutableSet<String> idColumns,
                          @Nonnull String idColumnsSeparator, boolean exclusive,
                          @Nonnull UriGeneratorType uriGeneratorType) {
        this.tableName = tableName;
        this.column2uri = column2uri;
        this.fallbackPrefix = fallbackPrefix;
        this.classes = classes;
        this.instancePrefix = instancePrefix;
        this.idColumns = idColumns;
        this.idColumnsSeparator = idColumnsSeparator;
        this.exclusive = exclusive;
        this.uriGeneratorType = uriGeneratorType;
    }

    /* --- --- --- builder --- --- --- */

    public static class Builder {
        private @Nonnull final String tableName;
        private @Nonnull final Map<String, URI> column2uri = new HashMap<>();
        private @Nullable String fallbackPrefix = StdPlain.URI_PREFIX;
        private @Nonnull final Set<URI> classes = new HashSet<>();
        private @Nullable String instancePrefix = StdPlain.URI_PREFIX;
        private @Nonnull final Set<String> idColumns = new HashSet<>();
        private @Nonnull String idColumnsSeparator = "-";
        private boolean exclusive = true;
        private @Nullable UriGeneratorType uriGeneratorType = null;

        public Builder(@Nonnull String tableName) {
            this.tableName = tableName;
        }

        public @CanIgnoreReturnValue @Nonnull Builder exclusive(boolean value) {
            exclusive = value;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder column2uri(@Nonnull String column,
                                                                 @Nonnull URI uri) {
            this.column2uri.put(column, uri);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder fallbackPrefix(@Nullable String prefix) {
            this.fallbackPrefix = prefix;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder addClass(@Nonnull URI cls) {
            this.classes.add(cls);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder instancePrefix(@Nullable String prefix) {
            this.instancePrefix = prefix;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder addIdColumn(@Nonnull String column) {
            this.idColumns.add(column);
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder idColumnsSeparator(@Nonnull String sep) {
            this.idColumnsSeparator = sep;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull
        Builder uriGenerator(@Nonnull UriGeneratorType type) {
            this.uriGeneratorType = type;
            return this;
        }

        public @Nonnull ContextMapping build() {
            if (uriGeneratorType == null) {
                if (!idColumns.isEmpty())
                    uriGeneratorType = UriGeneratorType.CONCAT;
                else
                    uriGeneratorType = UriGeneratorType.BLANK;
            }
            return new ContextMapping(tableName, ImmutableMap.copyOf(column2uri), fallbackPrefix,
                                      ImmutableSet.copyOf(classes), instancePrefix,
                                      ImmutableSet.copyOf(idColumns), idColumnsSeparator,
                                      exclusive, uriGeneratorType);
        }
    }

    public static @Nonnull Builder builder(@Nonnull String tableName) {
        return new Builder(tableName);
    }

    /* --- --- --- parse --- --- --- */

    /**
     * See {@link ContextMapping#parse(InputStream, String)}.
     */
    public static @Nonnull ContextMapping parse(@Nonnull InputStream stream)
            throws IOException, ContextMappingParseException {
        return parse(stream, null);
    }

    /**
     * See {@link ContextMapping#parse(DictTree, String)}
     */
    public static @Nonnull ContextMapping parse(@Nonnull InputStream stream,
                                                @Nullable String tableName)
            throws IOException, ContextMappingParseException {
        return parse(DictTree.load().fromInputStream(stream), tableName);
    }

    /**
     * See {@link ContextMapping#parse(DictTree, String)}
     */
    public static @Nonnull ContextMapping parse(@Nonnull DictTree dictTree)
            throws ContextMappingParseException {
        return parse(dictTree, null);
    }

    /**
     * Parses the mapping from the already parsed YAML/JSON/JSON-LD source.
     *
     * @param d the parsed {@link DictTree}
     * @param tableName the name of the table (relevant for the interface methods).
     *                  This will <b>override</b> any <code>tableName</code> defined on
     *                  the context file.
     * @return The functional {@link ContextMapping}
     * @throws ContextMappingParseException If something is wrong with syntax of the values in
     *                                      the file (beyond JSON/YAML syntax)
     */
    public static @Nonnull ContextMapping parse(@Nonnull DictTree d,
                                                @Nullable String tableName)
            throws ContextMappingParseException {
        if (tableName == null)
            tableName = d.getString("@tableName", d.getString("tableName"));
        if (tableName == null)
            throw new ContextMappingParseException("No @tableName defined");
        Builder b = builder(tableName);

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
                    if (fallbackPrefix == null)
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
        return b.build();
    }

    /* --- --- --- internals --- --- --- */

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

    private @Nullable URI getUri(@Nonnull String column) {
        URI uri = column2uri.get(column);
        if (uri != null)
            return uri;
        if (fallbackPrefix != null)
            return new StdURI(fallbackPrefix+column);
        return null;
    }

    /* --- --- --- getters --- --- --- */
    public @Nonnull String getTableName() {
        return tableName;
    }
    public @Nonnull ImmutableMap<String, URI> getColumn2uri() {
        return column2uri;
    }
    public @Nullable String getFallbackPrefix() {
        return fallbackPrefix;
    }
    public @Nonnull ImmutableSet<URI> getClasses() {
        return classes;
    }
    public @Nullable String getInstancePrefix() {
        return instancePrefix;
    }
    public @Nonnull ImmutableSet<String> getIdColumns() {
        return idColumns;
    }
    public @Nonnull String getIdColumnsSeparator() {
        return idColumnsSeparator;
    }
    public boolean isExclusive() {
        return exclusive;
    }

    public @Nonnull UriGeneratorType getUriGeneratorType() {
        return uriGeneratorType;
    }

    /* --- --- --- interface implementation --- --- --- */

    @Override
    public @Nonnull Molecule createMolecule(@Nullable Map<String, List<String>> table2columns) {
        Collection<String> columns = column2uri.keySet();
        if (table2columns != null) {
            assert table2columns.keySet().size() == 1;
            if (!table2columns.containsKey(tableName))
                return Molecule.builder(tableName).build();
            columns = table2columns.get(tableName);
        } else if (fullMolecule != null) {
            return fullMolecule;
        }

        MoleculeBuilder b = Molecule.builder(tableName);
        if (!classes.isEmpty())
            b.out(rdfType, new Atom(tableName + "." + rdfType.getURI()));
        for (String column : columns) {
            URI uri = getUri(column);
            if (uri != null) {
                ColumnTag tag = new ColumnTag(new Column(tableName, column));
                Atom colAtom = Molecule.builder(tableName + "." + column).tag(tag).buildAtom();
                b.out(uri, colAtom, singletonList(tag));
            }
        }
        Molecule m = b.tag(new TableTag(tableName)).exclusive(exclusive).build();
        if (table2columns == null)
            fullMolecule = m;
        return m;
    }

    @Override
    public @Nonnull Set<String> getIdColumnsNames(@Nonnull String table,
                                                  @Nullable Collection<?> columns) {
        // linked resources not supported (yet), thus columns is ignored
        if (!this.tableName.equals(table)) {
            logger.warn("Bad name: {}. Expected {}", table, this.tableName);
            if (!this.tableName.toLowerCase().trim().equals(table.toLowerCase().trim()))
                return emptySet(); // do not tolerate
        }
        return idColumns;
    }

    @Override
    public @Nullable Term column2predicate(@Nonnull Column column) {
        if (!Objects.equals(column.table, tableName))
            return null; // extraneous table
        return getUri(column.column);
    }

    @Override
    public @Nonnull Term getNameFor(@Nonnull List<Column> columns, @Nonnull List<?> values) {
        return JenaWrappers.fromJena(createResource(null, columns, values));
    }

    @Override
    public int toRDF(@Nonnull Model model, @Nonnull List<Column> columns, @Nonnull List<?> values) {
        assert columns.stream().allMatch(c -> c.getTable().equals(tableName));

        int triples = 0;
        Resource r = createResource(model, columns, values);
        for (URI aClass : classes) {
            r.addProperty(RDF.type, JenaWrappers.toJena(aClass));
            ++triples;
        }

        for (int i = 0, size = columns.size(); i < size; i++) {
            Column column = columns.get(i);
            if (!column.table.equals(tableName)) continue;
            Property p = JenaWrappers.toJenaProperty(getUri(column.column));
            if (p != null) {
                Object value = values.get(i);
                if (value != null) {
                    r.addProperty(p, ResourceFactory.createTypedLiteral(value));
                    ++triples;
                }
            }
        }

        return triples;
    }

    private @Nonnull Resource createResource(@Nullable Model model, @Nonnull List<Column> columns,
                                             @Nonnull List<?> values) {
        if (uriGeneratorType == UriGeneratorType.BLANK) {
            if (model == null) return ResourceFactory.createResource();
            return model.createResource();
        } else if (uriGeneratorType == UriGeneratorType.SEQ) {
            assert instancePrefix != null : "Can only use generator SEQ if instancePrefix != null";
            if (model == null)
                return ResourceFactory.createResource(instancePrefix + (nextId.get()+1));
            return model.createResource(instancePrefix + nextId.incrementAndGet());
        } else {
            assert uriGeneratorType == UriGeneratorType.CONCAT
                    : "Unexpected uriGeneratorType="+uriGeneratorType;
            int size = columns.size();
            assert values.size() == size : "values and columns do not have the same size";
            StringBuilder b = new StringBuilder();
            if (instancePrefix != null)
                b.append(instancePrefix);
            for (String idColumn : idColumns) {
                for (int i = 0; i < size; i++) {
                    if (columns.get(i).column.equals(idColumn)) {
                        Object o = values.get(i);
                        if (o instanceof Lit)
                            o = ((Lit) o).getLexicalForm();
                        b.append(o).append(idColumnsSeparator);
                        break;
                    }
                }
            }
            if (!idColumns.isEmpty())
                b.setLength(b.length()-idColumnsSeparator.length());
            if (model == null)
                return ResourceFactory.createResource(b.toString());
            return model.createResource(b.toString());
        }
    }
}
