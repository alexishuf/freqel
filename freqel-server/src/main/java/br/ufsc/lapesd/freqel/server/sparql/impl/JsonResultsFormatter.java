package br.ufsc.lapesd.freqel.server.sparql.impl;

import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.server.sparql.FormattedResults;
import br.ufsc.lapesd.freqel.server.sparql.ResultsFormatter;
import com.google.common.collect.Sets;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class JsonResultsFormatter implements ResultsFormatter {
    private static final Logger logger = LoggerFactory.getLogger(JsonResultsFormatter.class);

    public static final @Nonnull MediaType SPARQL_JSON_TYPE =
            new MediaType("application", "sparql-results+json");

    @Override
    public @Nonnull Set<MediaType> outputMediaTypes() {
        return Sets.newHashSet(SPARQL_JSON_TYPE, APPLICATION_JSON_TYPE);
    }

    @Override
    public @Nonnull FormattedResults format(@Nonnull @WillClose Results results, boolean isAsk,
                                            @Nullable MediaType mediaType) {
        if (mediaType != null && !APPLICATION_JSON_TYPE.isCompatible(mediaType)
                && !SPARQL_JSON_TYPE.isCompatible(mediaType)) {
            throw new IllegalArgumentException("Unsupported MediaType "+mediaType);
        }
        return isAsk ? formatAsk(results) : formatResults(results);
    }

    private @Nonnull FormattedResults formatResults(@Nonnull @WillClose Results results) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (OutputStreamWriter byteWriter = new OutputStreamWriter(byteOut, UTF_8);
             JsonWriter writer = new JsonWriter(byteWriter)) {
            writer.beginObject();
            writer.name("head");
            writer.beginObject();
            writer.name("vars");
            writer.beginArray();
            for (String varName : results.getVarNames()) writer.value(varName);
            writer.endArray(); // vars
            writer.endObject(); // head
            writer.name("results");
            writer.beginObject();
            writer.name("bindings");
            writer.beginArray();
            while (results.hasNext())
                writeBinding(writer, results.next());
            writer.endArray(); // bindings
            writer.endObject(); // results
            writer.endObject(); // root
        } catch (IOException e) {
            throw new RuntimeException(e); //should never occur
        } finally {
            results.close();
        }
        return new FormattedResults(APPLICATION_JSON_TYPE, byteOut.toByteArray());
    }

    private void writeBinding(@Nonnull JsonWriter writer,
                              @Nonnull Solution next) throws IOException {
        writer.beginObject();
        for (String varName : next.getVarNames()) {
            Term term = next.get(varName);
            if (term != null) {
                if (!term.isLiteral() && term.isBlank() && !term.isURI()) {
                    logger.warn("Solution {} for var {} is not literal, nor blank nor URI!",
                            term, varName);
                    continue;
                }
                writer.name(varName);
                writer.beginObject();
                writer.name("type");
                if      (term.isURI()    ) writer.value("uri");
                else if (term.isLiteral()) writer.value("literal");
                else                       writer.value("blank");
                writer.name("value");
                if      (term.isURI()    ) writer.value(term.asURI().getURI());
                else if (term.isLiteral()) writer.value(term.asLiteral().getLexicalForm());
                else                       writer.value(term.asBlank().toString());
                if (term.isLiteral()) {
                    Lit lit = term.asLiteral();
                    if (lit.getLangTag() != null)
                        writer.name("xml:lang").value(lit.getLangTag());
                    else
                        writer.name("datatype").value(lit.getDatatype().getURI());
                }
                writer.endObject();
            }
        }
        writer.endObject();
    }

    private @Nonnull FormattedResults formatAsk(@Nonnull @WillClose Results results) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (OutputStreamWriter byteWriter = new OutputStreamWriter(byteOut, UTF_8);
             JsonWriter writer = new JsonWriter(byteWriter)) {
            writer.beginObject();
            writer.name("head").beginObject().endObject();
            writer.name("boolean").value(results.hasNext());
            writer.endObject(); // root
        } catch (IOException e) {
            throw new RuntimeException(e); //should never occur
        } finally {
            results.close();
        }
        return new FormattedResults(APPLICATION_JSON_TYPE, byteOut.toByteArray());
    }
}
