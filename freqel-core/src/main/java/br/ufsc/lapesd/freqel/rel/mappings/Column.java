package br.ufsc.lapesd.freqel.rel.mappings;

import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class Column {
    public final @Nonnull String table;
    public final @Nonnull String column;

    public Column(@Nonnull String table, @Nonnull String column) {
        this.table = table;
        this.column = column;
    }

    public @Nonnull String getTable() {
        return table;
    }

    public @Nonnull String getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return table+"."+column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Column)) return false;
        Column column1 = (Column) o;
        return getTable().equals(column1.getTable()) &&
                getColumn().equals(column1.getColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTable(), getColumn());
    }
}
