package br.ufsc.lapesd.freqel.jena.query.parse;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.jena.query.parse.impl.ConvertVisitor;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParserOptions;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParserService;
import br.ufsc.lapesd.freqel.query.parse.UnsupportedSPARQLFeatureException;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;

import javax.annotation.Nonnull;

public class JenaSPARQLParserService implements SPARQLParserService {
    @Override
    public @Nonnull Op parse(@Nonnull SPARQLParserOptions options,
                             @Nonnull String sparql) throws SPARQLParseException {
        try {
            return convert(options, QueryFactory.create(sparql));
        } catch (QueryParseException e) {
            throw new SPARQLParseException("SPARQL syntax error: "+e.getMessage(), e, sparql);
        }
    }

    public @Nonnull Op convert(@Nonnull SPARQLParserOptions options,
                               @Nonnull Query q) throws SPARQLParseException {
        ConvertVisitor visitor = new ConvertVisitor(options);
        try {
            q.visit(visitor);
            Op tree = visitor.getTree();
            assert tree.assertTreeInvariants();
            return tree;
        } catch (ConvertVisitor.FeatureException e) {
            throw new UnsupportedSPARQLFeatureException(e.getMessage(), q.serialize());
        } catch (RuntimeException e) {
            throw new SPARQLParseException("RuntimeException while parsing query: "+e.getMessage(),
                    e, q.serialize());
        }
    }
}
