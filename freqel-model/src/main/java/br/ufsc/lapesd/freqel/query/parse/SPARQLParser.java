package br.ufsc.lapesd.freqel.query.parse;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkState;

public class SPARQLParser {
    private final static String HIDDEN_VAR_PREFIX = "parserPathHiddenVar";
    private final static SPARQLParser INSTANCE = new SPARQLParser().lockConfiguration();
    private final static SPARQLParser TOLERANT = new SPARQLParser()
            .allowExtraProjections(true)
            .eraseGroupBy(true).eraseOrderBy(true).eraseOffset(true)
            .lockConfiguration();
    private final SPARQLParserOptions options = new SPARQLParserOptions();
    private boolean locked = false;
    private @Nullable SPARQLParserService service = null;

    public static @Nonnull StdVar hidden(int id) {
        return new StdVar(HIDDEN_VAR_PREFIX+id);
    }

    public @Nonnull SPARQLParser lockConfiguration() {
        locked = true;
        return this;
    }

    public @Nonnull SPARQLParser allowExtraProjections(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        options.setAllowExtraProjections(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseGroupBy(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        options.setEraseGroupBy(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseOrderBy(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        options.setEraseOrderBy(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseOffset(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        options.setEraseOffset(value);
        return this;
    }

    public static @Nonnull SPARQLParser tolerant() {
        return TOLERANT;
    }

    public static @Nonnull SPARQLParser strict() {
        return INSTANCE;
    }

    /* ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- */

    private @Nonnull SPARQLParserService getService() {
        ServiceLoader<SPARQLParserService> loader = ServiceLoader.load(SPARQLParserService.class);
        SPARQLParserService first = null;
        for (SPARQLParserService svc : loader) {
            if (first == null || svc.toString().compareTo(first.toString()) < 0)
                first = svc;
        }
        if (first == null) {
            throw new RuntimeException("No SPARQLParserService implementation found. "+
                                       "Consider adding freqel-jena to the classpath.");
        }
        return first;
    }

    public @Nonnull Op parse(@Nonnull String sparql) throws SPARQLParseException {
        return getService().parse(options, sparql);
    }

    public @Nonnull Op parse(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        try {
            StringBuilder b = new StringBuilder();
            for (int c = sparqlReader.read(); c >= 0; c = sparqlReader.read())
                b.append((char)c);
            return parse(b.toString());
        } catch (IOException e) {
            throw new SPARQLParseException("Failed to read SPARQL from reader", e, null);
        }
    }

    public @Nonnull Op parse(@Nonnull InputStream stream) throws SPARQLParseException {
        return parse(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    public @Nonnull Op parse(@Nonnull File file) throws SPARQLParseException, IOException {
        try (FileInputStream stream = new FileInputStream(file)) {
            return parse(stream);
        }
    }

    private @Nonnull CQuery getConjunctive(@Nonnull Op op) throws SPARQLParseException {
        if (op instanceof QueryOp)
            return ((QueryOp) op).getQuery();
        throw new SPARQLParseException("parseConjunctive received an input query that " +
                                       "is not conjunctive", SPARQLString.create(op).getSparql());
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull String sparql) throws SPARQLParseException {
        return getConjunctive(parse(sparql));
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        return getConjunctive(parse(sparqlReader));
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull InputStream stream) throws SPARQLParseException {
        return getConjunctive(parse(stream));
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull File file) throws SPARQLParseException, IOException {
        return getConjunctive(parse(file));
    }
}
