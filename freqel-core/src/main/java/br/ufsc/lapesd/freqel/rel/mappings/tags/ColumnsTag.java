package br.ufsc.lapesd.freqel.rel.mappings.tags;

import br.ufsc.lapesd.freqel.description.molecules.tags.HybridTag;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

@Immutable
public class ColumnsTag implements HybridTag {
    private @Nonnull final String table;
    private @Nonnull final ImmutableList<Column> columns;
    private final boolean direct;

    public ColumnsTag(@Nonnull List<Column> columns) {
        this(columns, false);
    }

    public ColumnsTag(@Nonnull List<Column> columns, boolean direct) {
        Preconditions.checkArgument(!columns.isEmpty());
        this.table =  columns.get(0).getTable();
        assert columns.stream().map(Column::getTable).allMatch(table::equals)
                : "Some columns from different tables are mixed together";
        this.columns = ImmutableList.copyOf(columns);
        assert new HashSet<>(columns).size() == columns.size() : "There are duplicate columns";
        assert !direct || columns.size() == 1 : "Direct ColumnTag with more than one column!";
        this.direct = direct;
    }

    public static @Nonnull ColumnsTag direct(@Nonnull Column column) {
        return new ColumnsTag(singletonList(column), true);
    }
    public static @Nonnull ColumnsTag direct(@Nonnull String table, @Nonnull String column) {
        return new ColumnsTag(singletonList(new Column(table, column)), true);
    }
    public static @Nonnull ColumnsTag nonDirect(@Nonnull String table, @Nonnull String column) {
        return new ColumnsTag(singletonList(new Column(table, column)), false);
    }

    @Override public @Nonnull String shortDisplayName() {
        return "columns";
    }

    public @Nonnull String getTable() {
        return table;
    }
    public boolean isDirect() {
        return direct;
    }
    public @Nonnull ImmutableList<Column> getColumns() {
        return columns;
    }

    public @Nonnull Stream<Column> stream() {
        return columns.stream();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnsTag)) return false;
        ColumnsTag columnsTag = (ColumnsTag) o;
        return isDirect() == columnsTag.isDirect() &&
                getColumns().equals(columnsTag.getColumns());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getColumns(), isDirect());
    }

    @Override
    public @Nonnull String toString() {
        if (direct) {
            assert columns.size() == 1;
            return "[direct]" + columns.get(0).toString();
        } else {
            assert columns.stream().map(Column::getTable).allMatch(table::equals);
            if (columns.size() == 1) {
                return columns.get(0).toString();
            } else {
                StringBuilder b = new StringBuilder();
                b.append(table).append('.');
                for (Column c : columns)
                    b.append(c.getColumn()).append(", ");
                b.setLength(b.length()-2);
                return b.toString();
            }
        }
    }
}
