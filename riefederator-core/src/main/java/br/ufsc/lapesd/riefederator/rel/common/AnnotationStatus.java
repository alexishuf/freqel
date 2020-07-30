package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AnnotationStatus {
    private final int missingTable, missingColumn, badColumn, size;
    private @Nonnull CQuery query;

    public AnnotationStatus(@Nonnull CQuery query) {
        this.query = query;
        int missingTable = 0, missingColumns = 0, badColumn = 0;
        this.size = query.size();
        Map<Term, String> s2table = new HashMap<>();
        for (Triple triple : query) {
            Term s = triple.getSubject();
            String table = s2table.computeIfAbsent(s, k -> StarsHelper.findTable(query, s));
            if (table == null)
                ++missingTable;
            Set<Column> columns = StarsHelper.getColumns(query, table, triple.getObject());
            if (columns.isEmpty()) {
                if (!StarsHelper.isPostRelational(query, triple.getObject()))
                    ++missingColumns;
            } else if (table != null) {
                if (columns.stream().noneMatch(c -> c.getTable().equals(table)))
                ++badColumn;
            }
        }
        this.missingTable = missingTable;
        this.missingColumn = missingColumns;
        this.badColumn = badColumn;
    }

    public boolean isValid() {
        return missingTable == 0 && missingColumn == 0 && badColumn == 0;
    }

    public boolean isEmpty() {
        return missingTable == size;
    }

    public void checkNotPartiallyAnnotated() {
        if (isValid() || isEmpty()) return;
        throw new IllegalArgumentException("Query is partially annotated! It misses " +
                "table annotation on "+missingTable+" triples, misses column " +
                "annotations on "+missingColumn+" triples and has "+badColumn +
                " ColumnTags not matching the TableTag. Query: "+query);
    }
}
