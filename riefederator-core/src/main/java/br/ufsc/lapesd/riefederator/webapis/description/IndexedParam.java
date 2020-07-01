package br.ufsc.lapesd.riefederator.webapis.description;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexedParam {
    public static final @Nonnull Pattern RX =
            Pattern.compile("^(.*)\\$riefederator\\[(\\d+)]");

    public final @Nonnull String base, index;

    public IndexedParam(@Nonnull String base, @Nonnull String index) {
        this.base = base;
        this.index = index;
    }

    public static @Nullable IndexedParam parse(@Nonnull String string) {
        Matcher matcher = RX.matcher(string);
        if (!matcher.matches()) return null;
        return new IndexedParam(matcher.group(1), matcher.group(2));
    }

    public static @Nonnull String getBase(@Nonnull String string) {
        Matcher matcher = RX.matcher(string);
        return matcher.matches() ? matcher.group(1) : string;
    }

    public static @Nonnull String index(@Nonnull String base, int index) {
        assert !RX.matcher(base).matches();
        return String.format("%s$riefederator[%d]", base, index);
    }

    public @Nonnull String getBase() {
        return base;
    }

    public @Nonnull String getIndex() {
        return index;
    }

    public int getIndexValue() {
        return Integer.parseInt(index);
    }

    @Override
    public String toString() {
        return String.format("%s$riefederator[%s]", base, index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexedParam)) return false;
        IndexedParam that = (IndexedParam) o;
        return getBase().equals(that.getBase()) &&
                getIndex().equals(that.getIndex());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBase(), getIndex());
    }
}
