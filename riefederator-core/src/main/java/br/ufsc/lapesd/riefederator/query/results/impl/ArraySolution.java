package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Immutable
public class ArraySolution extends AbstractSolution {
    private final @Nonnull IndexedSet<String> vars;
    private final @Nonnull Term[] values;
    public static final @Nonnull ArraySolution EMPTY
            = new ArraySolution(IndexedSet.empty(), new Term[0]);


    protected ArraySolution(@Nonnull IndexedSet<String> vars, @Nonnull Term[] values) {
        this.vars = vars;
        this.values = values;
    }

    @Override
    public Term get(@Nonnull String varName, Term fallback) {
        int idx = vars.indexOf(varName);
        return idx < 0 ? fallback : values[idx];
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        for (int i = 0; i < vars.size(); i++)
            consumer.accept(vars.get(i), values[i]);
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return vars;
    }

//    @Override
//    public int hashCode() {
//        int local = hashCache;
//        if (local == 0) {
//            local = 17;
//            for (int i = 0; i < vars.size(); i++) {
//                Term term = values[i];
//                int termCode = term == null ? 17 : term.hashCode();
//                local = (local * 37 + vars.get(i).hashCode()) * 37 + termCode;
//            }
//            hashCache = local;
//        }
//        return local;
//    }
//
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ArraySolution) {
            ArraySolution rhs = (ArraySolution) obj;
            if (hashCache != rhs.hashCache) return false;
            if (!vars.equals(rhs.vars)) return false;
            int size = vars.size();
            for (int i = 0; i < size; i++) {
                if (!Objects.equals(values[i], rhs.values[i])) return false;
            }
            return true;
        }
        return super.equals(obj);
    }

    public static @Nonnull ValueFactory forVars(@Nonnull Collection<String> vars) {
        IndexedSet<String> indexedSet = IndexedSet.fromDistinct(vars);
        assert new ArrayList<>(indexedSet).equals(new ArrayList<>(vars)); //order must be preserved!
        return new ValueFactory(indexedSet);
    }

    public static class ValueFactory  {
        private final @Nonnull IndexedSet<String> vars;

        public ValueFactory(@Nonnull IndexedSet<String> vars) {
            this.vars = vars;
        }

        public @Nonnull ArraySolution fromValues(@Nonnull Collection<Term> collection) {
            Preconditions.checkArgument(collection.size() == vars.size());
            Term[] values = collection.toArray(new Term[0]);
            return new ArraySolution(vars, values);
        }

        public @Nonnull ArraySolution fromFunction(@Nonnull Function<String, Term> function) {
            int size = vars.size();
            Term[] values = new Term[size];
            for (int i = 0; i < size; i++)
                values[i] = function.apply(vars.get(i));
            return new ArraySolution(vars, values);
        }
    }
}
