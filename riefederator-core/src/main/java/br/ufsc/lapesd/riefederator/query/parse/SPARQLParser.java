package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.parse.impl.ConvertOptions;
import br.ufsc.lapesd.riefederator.query.parse.impl.ConvertVisitor;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkState;

public class SPARQLParser {
    private final static String HIDDEN_VAR_PREFIX = "parserPathHiddenVar";
    private final static SPARQLParser INSTANCE = new SPARQLParser().lockConfiguration();
    private final static SPARQLParser TOLERANT = new SPARQLParser()
            .allowExtraProjections(true)
            .eraseGroupBy(true).eraseOrderBy(true).eraseOffset(true)
            .lockConfiguration();
    private ConvertOptions convertOptions = new ConvertOptions();
    private boolean locked = false;

    public static @Nonnull StdVar hidden(int id) {
        return new StdVar(HIDDEN_VAR_PREFIX+id);
    }

    public @Nonnull SPARQLParser lockConfiguration() {
        locked = true;
        return this;
    }

    public @Nonnull SPARQLParser allowExtraProjections(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        convertOptions.setAllowExtraProjections(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseGroupBy(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        convertOptions.setEraseGroupBy(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseOrderBy(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        convertOptions.setEraseOrderBy(value);
        return this;
    }

    public @Nonnull SPARQLParser eraseOffset(boolean value) {
        checkState(!locked, "SPARQLQueryParser configuration is locked");
        convertOptions.setEraseOffset(value);
        return this;
    }

    public static @Nonnull SPARQLParser tolerant() {
        return TOLERANT;
    }

    public static @Nonnull SPARQLParser strict() {
        return INSTANCE;
    }

    /* ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- */

    public @Nonnull Op parse(@Nonnull String sparql) throws SPARQLParseException {
        try {
            return convert(QueryFactory.create(sparql));
        } catch (QueryParseException e) {
            throw new SPARQLParseException("SPARQL syntax error: "+e.getMessage(), e, sparql);
        }
    }

    public @Nonnull Op parse(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        try {
            return parse(IOUtils.toString(sparqlReader));
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


    public @Nonnull CQuery parseConjunctive(@Nonnull String sparql) throws SPARQLParseException {
        try {
            Op root = convert(QueryFactory.create(sparql));
            if (root instanceof QueryOp)
                return ((QueryOp) root).getQuery();
            throw new SPARQLParseException("parseConjunctive received an input query that " +
                                           "is not conjunctive", sparql);
        } catch (QueryParseException e) {
            throw new SPARQLParseException("SPARQL syntax error: "+e.getMessage(), e, sparql);
        }
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull Reader sparqlReader) throws SPARQLParseException {
        try {
            return parseConjunctive(IOUtils.toString(sparqlReader));
        } catch (IOException e) {
            throw new SPARQLParseException("Failed to read SPARQL from reader", e, null);
        }
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull InputStream stream) throws SPARQLParseException {
        return parseConjunctive(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    public @Nonnull CQuery parseConjunctive(@Nonnull File file) throws SPARQLParseException, IOException {
        try (FileInputStream stream = new FileInputStream(file)) {
            return parseConjunctive(stream);
        }
    }

    public @Nonnull Op convert(@Nonnull Query q) throws SPARQLParseException {
        ConvertVisitor visitor = new ConvertVisitor(convertOptions);
        try {
            q.visit(visitor);
            return visitor.getTree();
        } catch (ConvertVisitor.FeatureException e) {
            throw new UnsupportedSPARQLFeatureException(e.getMessage(), q);
        } catch (RuntimeException e) {
            throw new SPARQLParseException("RuntimeException while parsing query: "+e.getMessage(),
                                           e, q.serialize());
        }
    }
}
