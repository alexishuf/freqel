package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.parsers.TermSerializer;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;

public class OnlyNumbersTermSerializer implements TermSerializer {
    public static final @Nonnull OnlyNumbersTermSerializer INSTANCE
            = builder().build();
    private final int width, slice;
    private final char fill;
    private final @Nonnull String formatString;

    public OnlyNumbersTermSerializer(int width, char fill, int slice) {
        Preconditions.checkArgument(fill == ' ' || fill == '0');
        this.width = width;
        this.fill = fill;
        this.slice = slice;
        this.formatString = "%" + (width < 0 ? "-" : "") +
                            (fill == '0' && width != 0 ? "0" : "") +
                            (width == 0 ? "" : Math.abs(width)) + "d";
    }

    public static class Builder {
        private int width = 0, slice = 0;
        private char fill = '0';

        public @Nonnull Builder setWidth(int width) {
            this.width = width;
            return this;
        }

        public @Nonnull Builder setSlice(int slice) {
            this.slice = slice;
            return this;
        }

        public @Nonnull Builder setFill(char fill) {
            this.fill = fill;
            return this;
        }

        public @Nonnull OnlyNumbersTermSerializer build() {
            return new OnlyNumbersTermSerializer(width, fill, slice);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    public int getWidth() {
        return width;
    }

    public int getSlice() {
        return slice;
    }

    public char getFill() {
        return fill;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("OnlyNumbersTermSerializer(%s slice=%d)", formatString, getSlice());
    }

    @Override
    public @Nonnull String toString(@Nonnull Term term, @Nullable String paramName,
                                    @Nullable APIRequestExecutor executor)
                throws NoTermSerializationException {
        String string = SimpleTermSerializer.INSTANCE.toString(term, paramName, executor);
        String clean = string.replaceAll("\\D", "");
        if (width == 0 && Math.abs(slice) <= clean.length())
            return getSlice(clean);
        BigInteger value = clean.isEmpty() ? BigInteger.ZERO : new BigInteger(clean);
        return getSlice(String.format(formatString, value));
    }

    private @Nonnull String getSlice(@Nonnull String string) {
        if (slice > 0)
            return string.substring(0, Math.min(slice, string.length()));
        else if (slice < 0)
            return string.substring(Math.max(string.length()+slice, 0));
        else
            return string;
    }
}
