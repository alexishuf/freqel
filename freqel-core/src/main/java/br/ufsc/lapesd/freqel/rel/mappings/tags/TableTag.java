package br.ufsc.lapesd.freqel.rel.mappings.tags;

import br.ufsc.lapesd.freqel.description.molecules.tags.AtomTag;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class TableTag  implements AtomTag {
    public final @Nonnull String table;

    public TableTag(@Nonnull String table) {
        this.table = table;
    }

    public @Nonnull String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableTag)) return false;
        TableTag tableTag = (TableTag) o;
        return getTable().equals(tableTag.getTable());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTable());
    }

    @Override
    public String toString() {
        return table;
    }
}
