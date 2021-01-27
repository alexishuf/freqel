package br.ufsc.lapesd.freqel.rel.mappings;

import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.jena.rdf.model.Model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

/**
 * Defines a mapping from relational/columnar data to RDF.
 * Provides Molecule creation and conversion of non-RDF data into RDF.
 *
 * <b>Implementer notes</b>: <code>createMolecule()</code>, <code>getNameFor</code> and
 * <code>toRDF</code> overloads all convert into one another. An implementing class only needs
 * to implement one of the overloads. Not implementing any overload will cause a
 * {@link StackOverflowError} at runtime due to infinite recursion
 */
public interface RelationalMapping {
    /**
     * Gets a molecule for all known tables and columns.
     *
     * The default implementation delegates to {@link RelationalMapping#createMolecule(Map)}.
     *
     * @return a Molecule with the RDF view of all tables and columns known
     */
    default @Nonnull Molecule createMolecule() {
        return createMolecule((Map<String, List<String>>)null);
    }

    /**
     * Gets a molecule including only the given columns.
     *
     * The default implementation delegates to {@link RelationalMapping#createMolecule(Map)}.
     *
     * @param columns the set of columns to include in the molecule. This implicitly defines a
     *                set of tables as well. If null all columns of all tables are included.
     * @return a Molecule with the RDF view of the given tables/columns
     */
    default @Nonnull Molecule createMolecule(@Nullable Collection<Column> columns) {
        if (columns == null)
            return createMolecule((Map<String, List<String>>)null);
        Map<String, List<String>> table2columns = new HashMap<>();
        for (Column column : columns) {
            List<String> list = table2columns.computeIfAbsent(column.table, k -> new ArrayList<>());
            list.add(column.column);
        }
        return createMolecule(table2columns);
    }

    /**
     * Gets a molecule including only the given columns of the given tables.
     *
     * The default implementation delegates to {@link RelationalMapping#createMolecule(Collection)}.
     *
     * @param table2columns a map of all tables to include into the columns to include in each
     *                      table. If a table maps to <code>null</code>, then all columns of that
     *                      table are included. If this parameter is <code>null</code> all
     *                      columns of all tables are included.
     * @return a Molecule with the RDF view of the given tables/columns
     */
    default @Nonnull Molecule createMolecule(@Nullable Map<String, List<String>> table2columns) {
        if (table2columns == null)
            return createMolecule((Collection<Column>)null);
        return createMolecule(table2columns.keySet().stream()
                     .flatMap(t -> table2columns.get(t).stream().map(c -> new Column(t, c)))
                     .collect(toList()));
    }

    /**
     * Gets a molecule including only the given columns of the given table.
     *
     * The default implementation delegates to {@link RelationalMapping#createMolecule(Map)}.
     *
     * @param table the table to map
     * @param columns the columns of the table to include. If null, all columns are included
     * @return a Molecule with the RDF view of the given columns of the table
     */
    default @Nonnull Molecule createMolecule(@Nonnull String table,
                                             @Nullable Collection<String> columns) {
        List<String> list = columns == null ? null
                          : columns instanceof List ? (List<String>) columns
                                                    : new ArrayList<>(columns);
        Map<String, List<String>> map = new HashMap<>();
        map.put(table, list);
        return createMolecule(map);
    }

    /**
     * Gets the (possibly empty) set of columns used to build subject URIs for the given table
     *
     * @param table the table of which a record will originate a RDF subject
     * @param columns the set of columns ({@link String} or {@link Column} instances)
     *                that are used in a query. This may be null or empty, in which case it will
     *                be ignored. If the underlying mapping further normalizes the source table
     *                yielding multiple subjects for each record, this parameter allows
     *                identifiying which sub-table is being matched.
     * @return Set of column names of table. Use
     *         {@link RelationalMapping#getIdColumns(String, Collection)} to get {@link Column}
     *         instances.
     */
    default @Nonnull Collection<String> getIdColumnsNames(@Nonnull String table,
                                                          @Nullable Collection<?> columns) {
        return getIdColumns(table, columns).stream().map(Column::getColumn).collect(toList());
    }
    /** See {@link RelationalMapping#getIdColumnsNames(String, Collection)} */
    default @Nonnull Collection<Column> getIdColumns(@Nonnull String table,
                                                     @Nullable Collection<?> columns) {
        return getIdColumnsNames(table, columns).stream()
                .map(n -> new Column(table, n)).collect(toList());
    }
    /** See {@link RelationalMapping#getIdColumnsNames(String, Collection)} */
    default @Nonnull Collection<String> getIdColumnsNames(@Nonnull String table) {
        return getIdColumnsNames(table, null);
    }
    /** See {@link RelationalMapping#getIdColumnsNames(String, Collection)} */
    default @Nonnull Collection<Column> getIdColumns(@Nonnull String table) {
        return getIdColumns(table, null);
    }

    /**
     * Gets the subject Term for the record composed of the given values.
     *
     * The default implementation delegates to {@link RelationalMapping#getNameFor(List, List)}.
     *
     * @param values values that compose a record
     * @return the Term (URI or blank node)
     */
    default @Nonnull Term getNameFor(@Nonnull Map<Column, Object> values) {
        ArrayList<Column> columns = new ArrayList<>(values.keySet());
        ArrayList<Object> list = new ArrayList<>();
        for (Column column : columns)
            list.add(values.get(column));
        return getNameFor(columns, list);
    }

    /**
     * Gets the subject Term for the record composed of the given values.
     *
     * The default implementation delegates to {@link RelationalMapping#getNameFor(Map)}.
     *
     * @param values values that compose a record
     * @return the Term (URI or blank node)
     */
    default @Nonnull Term getNameFor(@Nonnull List<Column> columns, @Nonnull List<?> values) {
        checkArgument(columns.size() == values.size(), "#columns != #values");
        Map<Column, Object> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            map.put(columns.get(i), values.get(i));
        return getNameFor(map);
    }

    /**
     * Gets the subject Term for the record composed of the given values.
     *
     * The default implementation delegates to {@link RelationalMapping#getNameFor(List, List)}.
     *
     * @param values values that compose a record
     * @return the Term (URI or blank node)
     */
    default @Nonnull Term getNameFor(@Nonnull String table, @Nonnull List<String> columns, @Nonnull List<?> values) {
        List<Column> list = columns.stream().map(c -> new Column(table, c)).collect(toList());
        return getNameFor(list, values);
    }

    /**
     * Gets the subject Term for the record composed of the given values.
     *
     * The default implementation delegates to {@link RelationalMapping#getNameFor(Map)}.
     *
     * @param values values that compose a record
     * @return the Term (URI or blank node)
     */
    default @Nonnull Term getNameFor(@Nonnull String table, @Nonnull Map<String, Object> values) {
        Map<Column, Object> map = new HashMap<>();
        for (Map.Entry<String, Object> e : values.entrySet())
            map.put(new Column(table, e.getKey()), e.getValue());
        return getNameFor(map);
    }

    /**
     * Write a RDF representation of values into model.
     *
     * The set of Object's mapped to a set of columns that shares the same {@link Column#table}
     * will yield a RDF subject. In handling multiple instances, use
     * {@link RelationalMapping#toRDF(Model, List)}
     *
     * The default implementation delegates to {@link RelationalMapping#toRDF(Model, List, List)}.
     *
     * @param model where to write triples
     * @param values values associated to columns of tables
     * @return number of triples written
     */
    @CanIgnoreReturnValue
    default int toRDF(@Nonnull Model model, @Nonnull Map<Column, Object> values) {
        ArrayList<Column> columns = new ArrayList<>(values.keySet());
        ArrayList<Object> list = new ArrayList<>();
        for (Column column : columns)
            list.add(values.get(column));
        return toRDF(model, columns, list);
    }

    /**
     * Write a RDF representation of the instances described by the columns and respective values.
     *
     * The default implementation delegates to {@link RelationalMapping#toRDF(Model, Map)}
     *
     * @param model  where to write the triples
     * @param columns list of columns
     * @param values list of possibly null values, the i-th value corresponding to the i-th column
     * @throws IllegalArgumentException if the number of columns and values do not match
     * @return the number of triples written
     */
    @CanIgnoreReturnValue
    default int toRDF(@Nonnull Model model, @Nonnull List<Column> columns, @Nonnull List<?> values) {
        checkArgument(columns.size() == values.size(), "#columns != #values");

        Map<Column, Object> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++)
            map.put(columns.get(i), values.get(i));
        return toRDF(model, map);
    }

    /**
     * Write an RDF representation of a instance of a single table.
     *
     * Delegates to {@link RelationalMapping#toRDF(Model, List, List)}
     *
     * @param model where to write the triples
     * @param table name of the source table
     * @param columns list of columns
     * @param values a possibly null value for each column.
     *               The i-th value corresponds to the i-th column
     * @throws IllegalArgumentException if number of columns and values do not match
     * @return the number of triples written
     */
    @CanIgnoreReturnValue
    default int toRDF(@Nonnull Model model, @Nonnull String table, @Nonnull List<String> columns,
                      @Nonnull List<?> values) {
        List<Column> list = columns.stream().map(c -> new Column(table, c)).collect(toList());
        return toRDF(model, list, values);
    }

    /**
     * Write an RDF representation of the instance of table with values defined in the given map.
     *
     * Delegates to {@link RelationalMapping#toRDF(Model, Map)}
     *
     * @param model where to write the triples
     * @param table source relational table
     * @param values mapping from columns of table to the (possibly null) values
     * @return the number of triples written
     */
    @CanIgnoreReturnValue
    default int toRDF(@Nonnull Model model, @Nonnull String table,
                      @Nonnull Map<String, Object> values) {
        Map<Column, Object> map = new HashMap<>();
        for (Map.Entry<String, Object> e : values.entrySet())
            map.put(new Column(table, e.getKey()), e.getValue());
        return toRDF(model, map);
    }

    /* --- multi-instance variants --- */

    default int toRDF(@Nonnull Model model, @Nonnull List<Map<Column, Object>> values) {
        int total = 0;
        for (Map<Column, Object> value : values)
            total += toRDF(model, value);
        return total;
    }
}
