package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.List;

public interface ChunkedEncoder {
    /**
     * A list of media types that describe what is produced by this encoder.
     *
     * The first element in the list must be the preferred and most accurate media type, while
     * any following media types are possible aliases or media types listd for compatibility
     * with clients using not ideal media types.
     *
     * @return A non-null and non-empty list with non-null, non-wildcard, media types without
     *         parameters.
     */
    @Nonnull List<String> resultMediaTypes();

    /**
     * Create a Flux of {@link ByteBuf}s, allocated from the given allocator
     * that when concatenated produce the byte representation of the given {@link Results}
     * in the {@link ChunkedEncoder#resultMediaTypes()} with the given charset.
     *
     * <strong>Warning about charset</strong>: Some media types fix a particular charset
     * (e.g., application/sparql-results+json), and thus the charset parameter will be ignored.
     *
     * @param allocator where to get clean {@link ByteBuf}s
     * @param results sequence of {@link Solution}s to be serialized
     * @param isAsk if true, will use special syntax for representing boolean results
     * @param charset Override UTF-8 as the charset used to encode the serialization.
     * @return a non-null and non-empty Flux of non-null and non-empty {@link ByteBuf}s that
     *         when concatenated yield the byte representation of {@code results}.
     */
    @Nonnull Publisher<ByteBuf> encode(@Nonnull ByteBufAllocator allocator,
                                       @Nonnull Results results, boolean isAsk,
                                       @Nullable Charset charset);
}
