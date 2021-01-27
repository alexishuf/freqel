package br.ufsc.lapesd.freqel.jena.query;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.impl.AbstractSolution;
import br.ufsc.lapesd.freqel.util.ArraySet;
import org.apache.commons.collections4.Transformer;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

public class JenaSolution extends AbstractSolution {
    private final @Nonnull QuerySolution querySolution;
    private @Nullable Set<String> varNames;

    public static final class Factory implements Transformer<QuerySolution, JenaSolution> {
        private final @Nonnull Set<String> varNames;

        public Factory(@Nonnull Collection<String> distinctVarNames) {
            this.varNames = ArraySet.fromDistinct(distinctVarNames);
        }

        @Override
        public @Nonnull JenaSolution transform(@Nonnull QuerySolution input) {
            return new JenaSolution(input, varNames);
        }
    }

    public JenaSolution() {
        this(new QuerySolutionMap());
    }

    public JenaSolution(@Nonnull QuerySolution querySolution) {
        this(querySolution, null);
    }

    protected JenaSolution(@Nonnull QuerySolution querySolution, @Nullable Set<String> varNames) {
        this.querySolution = querySolution;
        this.varNames = varNames;
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
        if (varNames == null)
            varNames = ArraySet.fromDistinct(varNames());
        return varNames;
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        for (String name : getVarNames()) {
            Term term = get(name);
            consumer.accept(name, term);
        }
    }
}
