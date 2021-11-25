package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.model.term.Term;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CSVChunkedEncoder extends AbstractSVChunkedEncoder {
    private static final String CONTENT_TYPE = "text/csv";
    private static final Map<Charset, byte[]> quoteBytes = new ConcurrentHashMap<>();

    public CSVChunkedEncoder() {
        super(',', "\r\n");
    }

    @Override protected void writeVarName(@Nonnull ByteBuf bb, @Nonnull String name,
                                          @Nonnull Charset charset) {
        bb.writeCharSequence(name, charset);
    }

    @Override protected void writeTerm(@Nonnull ByteBuf bb, @Nonnull Term term,
                                       @Nonnull Charset charset) {
        byte[] quote = quoteBytes.computeIfAbsent(charset, "\""::getBytes);
        if (term.isURI()) {
            bb.writeCharSequence(term.asURI().getURI(), charset);
        } else if (term.isBlank()) {
            String name = term.asBlank().getName();
            bb.writeCharSequence("_:", charset);
            bb.writeCharSequence(name == null ? UUID.randomUUID().toString() : name, charset);
        } else {
            String lex = term.asLiteral().getLexicalForm();
            if (lex.indexOf('"') >= 0 || lex.indexOf(',') >= 0 || lex.indexOf('\n') >= 0) {
                bb.writeBytes(quote);
                int start = 0, end = lex.length();
                while (start < end) {
                    int i = lex.indexOf('"', start);
                    int matchEnd = i < 0 ? end : i+1;
                    bb.writeCharSequence(lex.subSequence(start, matchEnd), charset);
                    if (i >= 0)
                        bb.writeBytes(quote);
                    start = matchEnd;
                }
                bb.writeBytes(quote);
            } else {
                bb.writeCharSequence(lex, charset);
            }
        }
    }

    @Override @Nonnull public List<String> resultMediaTypes() {
        return Collections.singletonList(CONTENT_TYPE);
    }
}
