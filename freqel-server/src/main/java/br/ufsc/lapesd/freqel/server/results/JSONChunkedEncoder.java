package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.server.results.ChunkedEncoderUtils.toFlux;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static reactor.core.publisher.Mono.just;

public class JSONChunkedEncoder implements ChunkedEncoder {
    private static final String CONTENT_TYPE = "application/sparql-results+json";
    private static final byte @NonNull [] ASK_PROLOGUE =
            "{\"head\":{},\"boolean\":".getBytes(UTF_8);
    private static final byte[] ASK_TRUE = "true}".getBytes(UTF_8);
    private static final byte[] ASK_FALSE = "false}".getBytes(UTF_8);
    private static final byte[] ROWS_EPILOGUE = "]}}".getBytes(UTF_8);
    private static final byte[] TYPE = "\":{\"type\":\"".getBytes(UTF_8);
    private static final byte[] LITERAL = "literal\",\"value\":\"".getBytes(UTF_8);
    private static final byte[] URI     = "uri\",\"value\":\"".getBytes(UTF_8);
    private static final byte[] BLANK   = "blank\",\"value\":\"".getBytes(UTF_8);
    private static final byte[] LANG   = "\",\"xml:lang\":\"".getBytes(UTF_8);
    private static final byte[] DATATYPE   = "\",\"datatype\":\"".getBytes(UTF_8);
    private static final byte[] END_TERM   = "\"},".getBytes(UTF_8);

    @Override public @Nonnull List<String> resultMediaTypes() {
        return Arrays.asList(CONTENT_TYPE, "application/json");
    }

    @Override
    public @Nonnull Publisher<ByteBuf> encode(@Nonnull ByteBufAllocator allocator,
                                              @Nonnull Results results, boolean isAsk,
                                              @Nullable Charset ignored) {
        if (isAsk)
            return just(wrappedBuffer(ASK_PROLOGUE, results.hasNext() ? ASK_TRUE : ASK_FALSE));
        int capacityGuess = results.getVarNames().size()*128;
        capacityGuess &= 1 << (32-Integer.numberOfLeadingZeros(capacityGuess));
        return Flux.concat(just(createPrologue(allocator, results.getVarNames())),
                          toFlux(results).map(new Encoder(allocator, capacityGuess)),
                          just(Unpooled.wrappedBuffer(ROWS_EPILOGUE)));
    }

    private static class Encoder implements Function<Solution, ByteBuf> {
        private final @Nonnull ByteBufAllocator allocator;
        private final int capacity;
        private boolean firstSolution = true;

        private Encoder(@Nonnull ByteBufAllocator allocator, int capacity) {
            this.allocator = allocator;
            this.capacity = capacity;
        }

        @Override public @Nonnull ByteBuf apply(@Nonnull Solution solution) {
            ByteBuf bb = allocator.buffer(capacity);
            if (firstSolution) firstSolution = false;
            else               bb.writeByte(',');
            bb.writeByte('{');
            int oldWriterIndex = bb.writerIndex();
            for (String name : solution.getVarNames()) {
                Term term = solution.get(name);
                if (term == null)
                    continue;
                bb.writeByte('"').writeCharSequence(name, UTF_8);
                bb.writeBytes(TYPE);
                if (term.isURI()) {
                    bb.writeBytes(URI).writeCharSequence(term.asURI().getURI(), UTF_8);
                } else if (term.isLiteral()) {
                    Lit lit = term.asLiteral();
                    bb.writeBytes(LITERAL).writeCharSequence(lit.getLexicalForm(), UTF_8);
                    String langTag = lit.getLangTag();
                    if (langTag != null) {
                        bb.writeBytes(LANG).writeCharSequence(langTag, UTF_8);
                    } else {
                        String dt = lit.getDatatype().getURI();
                        if (!dt.equals(V.XSD.xstring.getURI()))
                            bb.writeBytes(DATATYPE).writeCharSequence(dt, UTF_8);
                    }
                } else {
                    bb.writeBytes(BLANK).writeCharSequence(term.toString(), UTF_8);
                }
                bb.writeBytes(END_TERM);
            }
            if (oldWriterIndex < bb.writerIndex())
                bb.writerIndex(bb.writerIndex()-1); // remove last ','
            bb.writeByte('}'); // close row
            return bb;
        }
    }

    private @Nonnull ByteBuf createPrologue(@Nonnull ByteBufAllocator allocator,
                                            @Nonnull Collection<String> varNames) {
        ByteBuf bb = allocator.buffer(21 + varNames.size()*16 + 30);
        bb.writeCharSequence("{\"head\":{\"vars\":[", UTF_8);
        for (String name : varNames) {
            bb.writeByte('"').writeCharSequence(name, UTF_8);
            bb.writeByte('"').writeByte(',');
        }
        if (!varNames.isEmpty())
            bb.writerIndex(bb.writerIndex()-1);
        bb.writeCharSequence("]},\"results\":{\"bindings\":[", UTF_8);
        return bb;
    }
}
