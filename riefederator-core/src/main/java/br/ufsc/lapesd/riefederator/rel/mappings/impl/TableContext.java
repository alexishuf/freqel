package br.ufsc.lapesd.riefederator.rel.mappings.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;

@Immutable
class TableContext {
    private static final Logger logger = LoggerFactory.getLogger(TableContext.class);
    private static final URI rdfType = new StdURI(RDF.type.getURI());

    private @Nonnull final String tableName;
    private @Nonnull final ImmutableMap<String, URI> column2uri;
    private @Nullable final String fallbackPrefix;
    private @Nonnull final ImmutableSet<URI> classes;
    private @Nullable final String instancePrefix;
    private @Nonnull final ImmutableList<String> idColumns;
    private @Nonnull final String idColumnsSeparator;
    private final boolean exclusive;
    private final @Nonnull ContextMapping.UriGeneratorType uriGeneratorType;

    public TableContext(@Nonnull String tableName,
                        @Nonnull ImmutableMap<String, URI> column2uri,
                        @Nullable String fallbackPrefix, @Nonnull ImmutableSet<URI> classes,
                        @Nullable String instancePrefix,
                        @Nonnull ImmutableList<String> idColumns,
                        @Nonnull String idColumnsSeparator, boolean exclusive,
                        @Nonnull ContextMapping.UriGeneratorType uriGeneratorType) {
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

    /* --- --- --- Plain getters --- --- --- */

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
    public @Nonnull ImmutableList<String> getIdColumns() {
        return idColumns;
    }
    public @Nonnull String getIdColumnsSeparator() {
        return idColumnsSeparator;
    }
    public boolean isExclusive() {
        return exclusive;
    }
    public @Nonnull ContextMapping.UriGeneratorType getUriGeneratorType() {
        return uriGeneratorType;
    }

    /* --- --- --- Utilities used by ContextMapping --- --- --- */

    public  @Nullable URI getUri(@Nonnull String column) {
        URI uri = column2uri.get(column);
        if (uri != null)
            return uri;
        if (fallbackPrefix != null)
            return new StdURI(fallbackPrefix+column);
        return null;
    }

    public @Nonnull MoleculeBuilder
    addCore(@Nullable MoleculeBuilder b, @Nullable Collection<String> columns) {
        if (columns == null)
            columns = column2uri.keySet();
        if (b == null) b = Molecule.builder(tableName);
        else           b.startNewCore(tableName);

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
        b.tag(new TableTag(tableName)).exclusive(exclusive);
        return b;
    }

    public int toRDF(@Nonnull AtomicLong nextId, @Nonnull Model model,
                     @Nonnull List<Column> columns, @Nonnull List<?> values) {
        assert columns.stream().allMatch(c -> c.getTable().equals(tableName));

        int triples = 0;
        Resource r = createResource(nextId, model, columns, values);
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

    public @Nonnull Resource
    createResource(@Nonnull AtomicLong nextId, @Nullable Model model,
                   @Nonnull List<Column> columns, @Nonnull List<?> values) {
        if (uriGeneratorType == ContextMapping.UriGeneratorType.BLANK) {
            if (model == null) return ResourceFactory.createResource();
            return model.createResource();
        } else if (uriGeneratorType == ContextMapping.UriGeneratorType.SEQ) {
            assert instancePrefix != null : "Can only use generator SEQ if instancePrefix != null";
            if (model == null)
                return ResourceFactory.createResource(instancePrefix + (nextId.get()+1));
            return model.createResource(instancePrefix + nextId.incrementAndGet());
        } else {
            assert uriGeneratorType == ContextMapping.UriGeneratorType.CONCAT
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

    /* --- --- --- Object methods --- --- --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableContext)) return false;
        TableContext that = (TableContext) o;
        return isExclusive() == that.isExclusive() &&
                getTableName().equals(that.getTableName()) &&
                getColumn2uri().equals(that.getColumn2uri()) &&
                Objects.equals(getFallbackPrefix(), that.getFallbackPrefix()) &&
                getClasses().equals(that.getClasses()) &&
                Objects.equals(getInstancePrefix(), that.getInstancePrefix()) &&
                getIdColumns().equals(that.getIdColumns()) &&
                getIdColumnsSeparator().equals(that.getIdColumnsSeparator()) &&
                getUriGeneratorType() == that.getUriGeneratorType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableName(), getColumn2uri(), getFallbackPrefix(), getClasses(),
                getInstancePrefix(), getIdColumns(), getIdColumnsSeparator(), isExclusive(),
                getUriGeneratorType());
    }

    @Override
    public String toString() {
        return "TableContext("+tableName+")";
    }
}
