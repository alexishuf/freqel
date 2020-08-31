package br.ufsc.lapesd.riefederator.linkedator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.linkedator.strategies.APIMoleculeInputsLinkedatorStrategy;
import br.ufsc.lapesd.riefederator.linkedator.strategies.LinkedatorStrategy;
import br.ufsc.lapesd.riefederator.model.SPARQLString;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.model.term.std.TemplateLink;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TemplateExpander;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.WillClose;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.*;

public class Linkedator {
    private static final Logger logger = LoggerFactory.getLogger(Linkedator.class);
    private static final @Nonnull Linkedator INSTANCE;

    static {
        INSTANCE = new Linkedator().register(new APIMoleculeInputsLinkedatorStrategy());
    }

    private final @Nonnull Set<LinkedatorStrategy> strategies = new HashSet<>();

    public @Nonnull static Linkedator getDefault() {
        return INSTANCE;
    }

    public @Nonnull Linkedator register(@Nonnull LinkedatorStrategy strategy) {
        strategies.add(strategy);
        return this;
    }

    public @Nonnull List<LinkedatorResult> getSuggestions(@Nonnull Collection<Source> sources) {
        List<LinkedatorResult> suggestions = new ArrayList<>();
        for (LinkedatorStrategy strategy : strategies) {
            Collection<LinkedatorResult> collection = strategy.getSuggestions(sources);
            assert new HashSet<>(collection).size() == collection.size() : "duplicate suggestions!";
            suggestions.addAll(collection);
        }
        assert new HashSet<>(suggestions).size() == suggestions.size()
                : "duplicate suggestions across strategies";
        return suggestions;
    }

    public void writeLinkedatorResults(@Nonnull @WillClose Writer writer,
                                       @Nonnull Collection<LinkedatorResult> results) {
        try (PrintWriter print = new PrintWriter(writer)) {
            for (LinkedatorResult result : results) {
                print.printf("confidence: %f\n", result.getConfidence());
                print.printf("strategy: %s\n", result.getStrategy());
                print.printf("template:\n");
                TemplateLink tpl = result.getTemplateLink();
                print.printf("  uri: %s\n", tpl.getURI());
                Term subject = tpl.getSubject(), object = tpl.getObject();
                assert subject.isVar();
                assert object.isVar();
                print.printf("  subject: %s\n", subject.asVar().getName());
                print.printf("  object: %s\n", object.asVar().getName());
                print.printf("  sparql: |\n");
                SPARQLString sparqlString = new SPARQLString(tpl.getTemplate());
                for (String line : Splitter.on('\n').split(sparqlString.getSparql()))
                    print.printf("    %s\n", line);
                print.println("---");
            }
        }
    }

    public void install(@Nonnull Federation federation,
                        @Nonnull List<LinkedatorResult> suggestions) {
        TemplateExpander expander = federation.getTemplateExpander();
        for (LinkedatorResult result : suggestions)
            expander.register(result.getTemplateLink());
    }

    public static class ResultParseException extends RuntimeException {
        public ResultParseException(String message) {
            super(message);
        }
        public ResultParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public @Nonnull Collection<LinkedatorResult>
    parseLinkedatorResults(@Nonnull Collection<DictTree> trees) throws ResultParseException {
        return parseLinkedatorResults(trees, false);
    }

    public @Nonnull Collection<LinkedatorResult>
    parseLinkedatorResults(@Nonnull Collection<DictTree> trees, boolean ignoreInvalid)
            throws ResultParseException {
        Set<LinkedatorResult> results = new HashSet<>();
        for (DictTree tree : trees) {
            try {
                results.add(parseLinkedatorResult(tree));
            } catch (ResultParseException e) {
                if (!ignoreInvalid) throw e;
                logger.error("Invalid Linkedator result at {}", tree);
            }
        }
        return results;
    }

    @VisibleForTesting
    LinkedatorResult parseLinkedatorResult(@Nonnull DictTree tree) throws ResultParseException {
        double confidence = tree.getDouble("confidence", 1.0);
        DictTree templateTree = tree.getMapNN("template");
        String uri = templateTree.getString("uri");
        if (uri == null)
            throw new ResultParseException("Missing uri key");
        uri = uri.replaceAll("^<(.*)>$", "$1");
        String subjectName = templateTree.getString("subject");
        if (subjectName == null)
            throw new ResultParseException("Missing subject key");
        String objectName = templateTree.getString("object");
        if (objectName == null)
            throw new ResultParseException("Missing object key");
        String queryString = templateTree.getString("sparql");
        if (queryString == null)
            throw new ResultParseException("Missing sparql key");
        CQuery query;
        try {
            Op root = SPARQLParser.strict().parse(queryString);
            if (root instanceof QueryOp) {
                query = ((QueryOp) root).getQuery();
            } else {
                throw new ResultParseException("Link templates must expand to conjunctive " +
                                               "queries. Query: " + queryString);
            }
        } catch (SPARQLParseException e) {
            throw new ResultParseException("SPARQL query failed to parse: " + e.getMessage() +
                                           "Query: "+ queryString, e);
        }
        StdVar sub = new StdVar(subjectName), obj = new StdVar(objectName);
        return new LinkedatorResult(new TemplateLink(uri, query, sub, obj), confidence);
    }
}
