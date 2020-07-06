package br.ufsc.lapesd.riefederator.webapis.description;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexedParam {
    public static final @Nonnull Pattern RX =
            Pattern.compile("^(.*)\\$riefederator\\[(\\d+)\\s*:\\s*(\\d+)]");

    public final @Nonnull String base, index;
    public final int size;

    public IndexedParam(@Nonnull String base, @Nonnull String index, int size) {
        this.base = base;
        this.index = index;
        this.size = size;
    }

    public static @Nullable IndexedParam parse(@Nonnull String string) {
        Matcher m = RX.matcher(string);
        if (!m.matches()) return null;
        return new IndexedParam(m.group(1), m.group(2), Integer.parseInt(m.group(3)));
    }

    public static @Nonnull String getBase(@Nonnull String string) {
        Matcher matcher = RX.matcher(string);
        return matcher.matches() ? matcher.group(1) : string;
    }

    public static @Nonnull String index(@Nonnull String base, int index, int size) {
        assert !RX.matcher(base).matches();
        return String.format("%s$riefederator[%d:%d]", base, index, size);
    }

    public @Nonnull String getBase() {
        return base;
    }

    public @Nonnull String getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }

    public int getIndexValue() {
        return Integer.parseInt(index);
    }

    @Override
    public String toString() {
        return String.format("%s$riefederator[%s:%d]", base, index, size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexedParam)) return false;
        IndexedParam that = (IndexedParam) o;
        return size == that.size &&
                getBase().equals(that.getBase()) &&
                getIndex().equals(that.getIndex());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBase(), getIndex(), size);
    }
}
