package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.impl.AbstractSolution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import org.apache.commons.collections4.Transformer;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;

public class JenaBindingSolution extends AbstractSolution {
    private final @Nonnull Binding binding;
    private final @Nonnull Set<String> varNames;

    public static final @Nonnull JenaBindingSolution EMPTY =
            new JenaBindingSolution(new BindingHashMap(), Collections.emptySet());

    public static final class Factory implements Transformer<Binding, JenaBindingSolution>,
                                                 Function<Binding, JenaBindingSolution> {
        private final @Nonnull Set<String> varNames;

        protected Factory(@Nonnull Set<String> varNames) {
            this.varNames = varNames;
        }

        @Override
        public JenaBindingSolution apply(Binding binding) {
            return new JenaBindingSolution(binding, varNames);
        }

        @Override
        public JenaBindingSolution transform(Binding input) {
            return new JenaBindingSolution(input, varNames);
        }
    }

    public static @Nonnull Factory forVars(@Nonnull Collection<String> varNames) {
        return new Factory(ArraySet.fromDistinct(varNames));
    }

    public JenaBindingSolution(@Nonnull Binding binding, @Nonnull Set<String> varNames) {
        this.binding = binding;
        this.varNames = varNames;
    }

    @Override
    public Term get(@Nonnull String varName, Term fallback) {
        Node node = binding.get(Var.alloc(varName));
        return node == null ? fallback : fromJena(node);
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        Iterator<Var> it = binding.vars();
        while (it.hasNext()) {
            Var var = it.next();
            Node node = binding.get(var);
            consumer.accept(var.getName(), node == null ? null : fromJena(node));
        }
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }
}
