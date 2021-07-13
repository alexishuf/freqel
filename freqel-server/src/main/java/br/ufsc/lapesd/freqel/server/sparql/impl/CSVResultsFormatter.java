package br.ufsc.lapesd.freqel.server.sparql.impl;

import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.server.sparql.FormattedResults;
import br.ufsc.lapesd.freqel.server.sparql.ResultsFormatter;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Set;

public class CSVResultsFormatter implements ResultsFormatter {
    public static final @Nonnull MediaType
            CSV_TYPE = new MediaType("text", "csv"),
            TAB_SV_TYPE = new MediaType("text", "tab-separated-values"),
            TSV_TYPE = new MediaType("text", "tsv");
    public static final @Nonnull Set<MediaType> TYPES =
            Sets.newHashSet(CSV_TYPE, TAB_SV_TYPE, TSV_TYPE);

    @FunctionalInterface
    private interface Serializer {
        void serialize(@Nullable Term term, @Nonnull Writer writer) throws IOException;
    }

    private static final @Nonnull Serializer CSV_SERIALIZER = (t, w) -> {
        if (t == null)
             return;
        if (t.isLiteral()) {
            String string = t.asLiteral().getLexicalForm();
            if (string.isEmpty()) {
                w.write(string);
                return;
            }
            int length = string.length();
            char first = string.charAt(0), last = string.charAt(length - 1);
            if (first < ' ' || last < ' ' || string.indexOf(',') >= 0) {
                for (int i = 0; i < length; i++) {
                    char c = string.charAt(i);
                    if (c == '"')
                        w.write('"');
                    w.write(c);
                }
                w.write('"');
            } else {
                w.write(string);
            }
        } else {
            w.write(t.toString(StdPrefixDict.EMPTY));
        }
    };
    private static final @Nonnull Serializer TSV_SERIALIZER = (t, w) -> {
        if (t == null)
            return;
        if (t.isBlank()) {
            w.write(t.toString());
        } else if (t.isLiteral()) {
            String string = t.asLiteral().toNT();
            int size = string.length();
            for (int i = 0; i < size; i++) {
                char c = string.charAt(i);
                switch (c) {
                    case '\t': w.append('\\').append('t'); break;
                    case '\n': w.append('\\').append('n'); break;
                    case '\r': w.append('\\').append('r'); break;
                    default: w.append(c);
                }
            }
        } else if (t.isURI()) {
            w.append('<').append(t.toString(StdPrefixDict.EMPTY)).append('>');
        } else {
            w.write(t.toString(StdPrefixDict.EMPTY));
        }
    };

    @Override
    public @Nonnull Set<MediaType> outputMediaTypes() {
        return TYPES;
    }

    private @Nonnull MediaType selectMediaType(@Nullable MediaType requestMediaType) {
        String charset = requestMediaType == null ? "UTF-8"
                : requestMediaType.getParameters().getOrDefault("charset", "utf-8").toUpperCase();
        if (requestMediaType != null && requestMediaType.isCompatible(CSV_TYPE))
            return CSV_TYPE.withCharset(charset);
        if (requestMediaType != null && TSV_TYPE.isCompatible(requestMediaType))
            return TSV_TYPE.withCharset(charset); // this MT is non-standard
        //fallback to the MT used in "SPARQL 1.1 Query Results CSV and TSV Formats"
        if (requestMediaType == null || TAB_SV_TYPE.isCompatible(requestMediaType))
            return  TAB_SV_TYPE.withCharset(charset);
        // requestMediaType != null  && incompatible with TAB_SV_TYPE
        throw new IllegalArgumentException("Unsupported MediaType "+requestMediaType);
    }

    private char delimiterFor(@Nonnull MediaType mediaType) {
        return mediaType.isCompatible(CSV_TYPE) ? ',' : '\t';
    }

    @Override
    public @Nonnull FormattedResults format(@Nonnull @WillClose Results results, boolean isAsk,
                                            @Nullable MediaType requestMediaType) {
        MediaType mediaType = selectMediaType(requestMediaType);
        Charset charset = Charset.forName(mediaType.getParameters().get("charset"));
        char del = delimiterFor(mediaType);
        String eol = del == '\t' ? "\n" : "\r\n";

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        Serializer serializer = del == ',' ? CSV_SERIALIZER : TSV_SERIALIZER;
        try (OutputStreamWriter writer = new OutputStreamWriter(byteOut, charset)) {
            Set<String> names = results.getVarNames();
            writeHeaders(writer, del, names);
            writer.write(eol);
            while (results.hasNext()) {
                Solution next = results.next();
                boolean first = true;
                for (String name : names) {
                    if (!first) writer.write(del);
                    else        first = false;
                    serializer.serialize(next.get(name), writer);
                }
                writer.write(eol);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); //should never occur
        } finally {
            results.close();
        }

        return new FormattedResults(mediaType, byteOut.toByteArray());
    }

    private void writeHeaders(@Nonnull Writer writer, char del,
                              @Nonnull Collection<String> varNames) throws IOException {
        boolean first = true;
        for (String varName : varNames) {
            if (!first) writer.append(del);
            else        first = false;
            if (del == '\t') writer.append('?');
            writer.append(varName);
        }
    }
}
