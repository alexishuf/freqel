package br.ufsc.lapesd.freqel.server.utils;

import com.google.common.net.PercentEscaper;

import javax.annotation.Nonnull;

public class PercentEncoder {
    @SuppressWarnings("UnstableApiUsage")
    public static @Nonnull String encode(@Nonnull String string) {
        return new PercentEscaper("", false).escape(string);
    }
}
