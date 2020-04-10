package br.ufsc.lapesd.riefederator.server.sparql.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.server.sparql.FormattedResults;
import br.ufsc.lapesd.riefederator.server.sparql.ResultsFormatter;
import com.google.common.collect.Sets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Set;

public class CSVResultsFormatter implements ResultsFormatter {
    private static final MediaType
            CSV_TYPE = new MediaType("text", "csv"),
            TAB_SV_TYPE = new MediaType("text", "tab-separated-values"),
            TSV_TYPE = new MediaType("text", "tsv");
    public static final @Nonnull Set<MediaType> TYPES =
            Sets.newHashSet(CSV_TYPE, TAB_SV_TYPE, TSV_TYPE);

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

    private @Nonnull CSVFormat toFormat(@Nonnull MediaType mediaType) {
        if (mediaType.isCompatible(CSV_TYPE))
            return CSVFormat.RFC4180;
        // text/tsv is non standard and only arises when using qonsole, which doesn't like \r\ns
        if (mediaType.isCompatible(TSV_TYPE) && !mediaType.isWildcardSubtype())
            return CSVFormat.RFC4180.withDelimiter('\t').withRecordSeparator('\n');
        else
            return CSVFormat.RFC4180.withDelimiter('\t');
    }

    private @Nonnull String serialize(boolean isCSV, @Nullable Term term) {
        if (term == null) return "";
        if (term.isBlank()) return term.toString();
        if (isCSV) {
            if (term.isLiteral()) return term.asLiteral().getLexicalForm();
            else if (term.isURI()) return term.asURI().getURI();
            else throw new IllegalArgumentException("Bad term type"+term.getType()+" for "+term);
        } else {
            if (term.isLiteral()) {
                return term.asLiteral().toNT().replaceAll("\t", "\\t")
                                              .replaceAll("\r", "\\r").replaceAll("\n", "\\n");
            }
            else if (term.isURI()) {
                return term.asURI().toNT();
            } else {
                throw new IllegalArgumentException("Bad term type"+term.getType()+" for "+term);
            }
        }
    }

    @Override
    public @Nonnull FormattedResults format(@Nonnull @WillClose Results results, boolean isAsk,
                                            @Nullable MediaType requestMediaType) {
        MediaType mediaType = selectMediaType(requestMediaType);
        Charset charset = Charset.forName(mediaType.getParameters().get("charset"));

        CSVFormat csvFormat = toFormat(mediaType);
        boolean isCsv = csvFormat.getDelimiter() == ',';
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (OutputStreamWriter writer = new OutputStreamWriter(byteOut, charset);
             CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
            ArrayList<String> headers = new ArrayList<>(results.getVarNames());
            printer.printRecord(headers);
            while (results.hasNext()) {
                Solution next = results.next();
                for (String header : headers)
                    printer.print(serialize(isCsv, next.get(header)));
                printer.println();
            }
        } catch (IOException e) {
            throw new RuntimeException(e); //should never occur
        } finally {
            results.close();
        }

        return new FormattedResults(mediaType, byteOut.toByteArray());
    }
}
