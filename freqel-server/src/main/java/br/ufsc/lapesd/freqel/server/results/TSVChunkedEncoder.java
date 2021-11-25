package br.ufsc.lapesd.freqel.server.results;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.term.Blank;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TSVChunkedEncoder extends AbstractSVChunkedEncoder {
    private static final String CONTENT_TYPE = "text/tab-separated-values";

    public TSVChunkedEncoder() {
        super('\t', "\n");
    }

    @Override
    protected void writeVarName(@Nonnull ByteBuf bb, @Nonnull String name,
                                @Nonnull Charset charset) {
        bb.writeByte('?').writeCharSequence(name, charset);
    }

    @Override
    protected void writeTerm(@Nonnull ByteBuf bb, @Nonnull Term term,
                             @Nonnull Charset charset) {
        if (term.isURI()) {
            bb.writeCharSequence(term.asURI().toNT(), charset);
        } else if (term.isLiteral()) {
            Lit lit = term.asLiteral();
            String nt = lit.toNT();
            if (lit.getDatatype().getURI().equals(V.XSD.xstring.getURI()))
                nt = nt.substring(0, nt.indexOf("\"^^<")+1);
            if (nt.indexOf('\t') >= 0 || nt.indexOf('\n') >= 0) {
                StringBuilder b = new StringBuilder(nt.length()+16);
                boolean escaped = false;
                for (int i = 0, len = nt.length(); i < len; i++) {
                    char c = nt.charAt(i);
                    if (escaped) {
                        escaped = false;
                        b.append('\\').append(c);
                    } else {
                        if      (c == '\t') b.append("\\t");
                        else if (c == '\n') b.append("\\n");
                        else if (c == '\\') escaped = true;
                        else                b.append(c);
                    }
                }
                nt = b.toString();
            }
            bb.writeCharSequence(nt, charset);
        } else if (term.isBlank()) {
            Blank blank = term.asBlank();
            bb.writeCharSequence("_:", charset);
            String name = blank.getName();
            bb.writeCharSequence(name == null ? UUID.randomUUID().toString() : name, charset);
        }
    }

    @Override public @Nonnull List<String> resultMediaTypes() {
        return Collections.singletonList(CONTENT_TYPE);
    }
}
