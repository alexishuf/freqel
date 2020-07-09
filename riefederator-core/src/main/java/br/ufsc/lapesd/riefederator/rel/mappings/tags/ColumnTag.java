package br.ufsc.lapesd.riefederator.rel.mappings.tags;

import br.ufsc.lapesd.riefederator.description.molecules.AtomTag;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeLinkTag;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class ColumnTag implements MoleculeLinkTag, AtomTag {
    private @Nonnull final Column column;

    public ColumnTag(@Nonnull Column column) {
        this.column = column;
    }

    public ColumnTag(@Nonnull String table, @Nonnull String column) {
        this(new Column(table, column));
    }

    public @Nonnull Column getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnTag)) return false;
        ColumnTag columnTag = (ColumnTag) o;
        return getColumn().equals(columnTag.getColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getColumn());
    }

    @Override
    public String toString() {
        return column.toString();
    }
}
