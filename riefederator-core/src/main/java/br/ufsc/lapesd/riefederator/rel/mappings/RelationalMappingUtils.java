package br.ufsc.lapesd.riefederator.rel.mappings;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toSet;

public class RelationalMappingUtils {
    /**
     * Get the set of {@link Column} instances that yield the given predicate in the molecule.
     *
     * This relies on annotations inserted on the molecule by implementations of
     * {@link RelationalMapping#createMolecule()} and related overloads. If the molecule is did
     * not originate from such method, then this function will return an empty set always.
     *
     * @param molecule the molecule from where to get the reverse mapping annotations
     * @param predicate the predicate to look up
     * @return A non-null distinct collection of Column instances, without nulls.
     */
    public static @Nonnull Collection<Column> predicate2column(@Nonnull Molecule molecule,
                                                               @Nonnull Term predicate) {
        return molecule.getIndex().stream(null, predicate, null)
                .flatMap(t -> t.getEdgeTags().stream())
                .filter(ColumnsTag.class::isInstance)
                .flatMap(t -> ((ColumnsTag) t).getColumns().stream())
                .collect(toSet());
    }

    public static String
    getTable(@Nonnull String method, @Nonnull Logger logger, @Nonnull Collection<Column> columns) {
        String table = null;
        if (columns.isEmpty()) {
            assert false : "Empty column set";
            logger.warn("No columns given to {}()", method);
            return null;
        }
        for (Column column : columns) {
            if (table == null) {
                table = column.getTable();
            } else if (!table.equals(column.getTable())) {
                assert false : "Columns have many tables, cannot determine which one to use";
                logger.warn("Creating a blank node since columns={} span multiple tables", columns);
                return null;
            }
        }
        return table;
    }

    public static String
    getTable(@Nonnull String method, @Nonnull Logger logger, @Nonnull List<Column> columns,
             @Nonnull List<?> values) {
        Preconditions.checkArgument(columns.size() == values.size(), "#columns != #values");
        if (columns.isEmpty()) {
            assert false : "Empty columns";
            logger.warn("No columns given to {}()!", method);
            return null;
        }
        return getTable(method, logger, columns);
    }
}
