package br.ufsc.lapesd.freqel.server.results;

import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ChunkedEncoderRegistry {
    private static final @Nonnull ChunkedEncoderRegistry INSTANCE;
    static {
        ChunkedEncoderRegistry registry = new ChunkedEncoderRegistry();
        registry.register(new TSVChunkedEncoder());
        registry.register(new JSONChunkedEncoder());
        registry.register(new CSVChunkedEncoder());
        INSTANCE = registry;
    }

    private final @NonNull Map<String, ChunkedEncoder> type2encoder = new HashMap<>();

    public static @Nonnull ChunkedEncoderRegistry get() {
        return INSTANCE;
    }

    /**
     * Register a {@link ChunkedEncoder} for later fetching
     * via {@link ChunkedEncoderRegistry#get(Object)}. Previous encoders registered for the same
     * type will be replaced with the new instance.
     *
     * @param encoder the {@link ChunkedEncoder}
     * @return this registry instance.
     */
    public @Nonnull ChunkedEncoderRegistry register(@Nonnull ChunkedEncoder encoder) {
        for (String type : encoder.resultMediaTypes())
            type2encoder.put(type, encoder);
        return this;
    }

    /**
     * Get the set of media types (without parameters) supported by encoders in the registry.
     *
     * @return a Set of non-null, non-empty and non-wildcard media types without parameters.
     */
    public @Nonnull Set<String> supportedTypes() {
        return type2encoder.keySet();
    }

    /**
     * Get a {@link ChunkedEncoder} for the given media type. type parameters will be ignored.
     *
     * @param mediaType the media type string or object whose {@link Object#toString()} yields
     *                  a media type. The media type cannot have wildcard type or subtype.
     * @return a compatible {@link ChunkedEncoder} or null if there is no registered
     *         {@link ChunkedEncoder} for the given mediaType.
     */
    public @Nullable ChunkedEncoder get(@Nonnull Object mediaType) {
        String string = mediaType.toString().trim().toLowerCase();
        int end = string.indexOf(' ');
        if (end >= 0)
            string = string.substring(0, end);
        return type2encoder.getOrDefault(string, null);
    }
}
