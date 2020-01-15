package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.impl.AbstractSolution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

public class JenaSolution extends AbstractSolution {
    private final @Nonnull QuerySolution querySolution;

    public JenaSolution() {
        this(new QuerySolutionMap());
    }

    public JenaSolution(@Nonnull QuerySolution querySolution) {
        this.querySolution = querySolution;
    }

    @Override
    public Term get(@Nonnull String varName, Term fallback) {
        RDFNode node = querySolution.get(varName);
        return node == null ? fallback : JenaWrappers.fromJena(node);
    }

    public @Nonnull Iterator<String> varNames() {
        return querySolution.varNames();
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        Set<String> set = new HashSet<>();
        varNames().forEachRemaining(set::add);
        return set;
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        for (Iterator<String> it = querySolution.varNames(); it.hasNext(); ) {
            String name = it.next();
            Term term = get(name);
            assert term != null;
            consumer.accept(name, term);
        }
    }
}
