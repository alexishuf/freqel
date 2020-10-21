package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.MutableSolution;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ImmFullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ArraySolution extends AbstractSolution implements MutableSolution {
    private final @Nonnull IndexSet<String> vars;
    private final @Nonnull Term[] values;
    public static final @Nonnull ArraySolution EMPTY
            = new ArraySolution(ImmFullIndexSet.empty(), new Term[0]);


    public ArraySolution(@Nonnull IndexSet<String> vars, @Nonnull Term[] values) {
        this.vars = vars;
        this.values = values;
    }

    @Override
    public Term get(@Nonnull String varName, Term fallback) {
        int idx = vars.indexOf(varName);
        return idx < 0 || values[idx] == null ? fallback : values[idx];
    }

    @Override @CanIgnoreReturnValue
    public @Nullable Term set(@Nonnull String varName, @Nullable Term value) {
        int idx = vars.indexOf(varName);
        if (idx < 0)
            throw new IllegalArgumentException(varName+" is not a variable of this ArraySolution");
        Term old = values[idx];
        values[idx] = value;
        return old;
    }

    @Override
    public @CanIgnoreReturnValue @Contract(" -> this") @Nonnull ArraySolution clear() {
        Arrays.fill(values, null);
        return this;
    }

    @Override
    public @CheckReturnValue @Nonnull ArraySolution copy() {
        Term[] copy = new Term[values.length];
        System.arraycopy(values, 0, copy, 0, values.length);
        return new ArraySolution(vars, copy);
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        assert vars.stream().noneMatch(Objects::isNull);
        for (int i = 0; i < vars.size(); i++) {
            if (values[i] != null)
                consumer.accept(vars.get(i), values[i]);
        }
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

    public static @Nonnull ArraySolution empty(@Nonnull Collection<String> vars) {
        IndexSet<String> indexSet = FullIndexSet.from(vars);
        return new ArraySolution(indexSet, new Term[indexSet.size()]);
    }

    public static @Nonnull ValueFactory forVars(@Nonnull Collection<String> vars) {
        IndexSet<String> indexSet = FullIndexSet.fromDistinct(vars);
        assert new ArrayList<>(indexSet).equals(new ArrayList<>(vars)); //order must be preserved!
        return new ValueFactory(indexSet);
    }

    public static class ValueFactory  {
        private final @Nonnull IndexSet<String> vars;

        public ValueFactory(@Nonnull IndexSet<String> vars) {
            this.vars = vars;
        }

        @CheckReturnValue
        public @Nonnull IndexSet<String> getVarNames() {
            return vars;
        }

        @CheckReturnValue
        public @Nonnull ArraySolution fromValues(@Nonnull Collection<Term> collection) {
            Preconditions.checkArgument(collection.size() == vars.size());
            Term[] values = collection.toArray(new Term[0]);
            return new ArraySolution(vars, values);
        }

        @CheckReturnValue
        public @Nonnull ArraySolution fromValues(@Nonnull Term... values) {
            return fromValues(Arrays.asList(values));
        }

        @CheckReturnValue
        public @Nonnull ArraySolution fromFunction(@Nonnull Function<String, Term> function) {
            int size = vars.size();
            Term[] values = new Term[size];
            for (int i = 0; i < size; i++)
                values[i] = function.apply(vars.get(i));
            return new ArraySolution(vars, values);
        }

        @CheckReturnValue
        public @Nonnull ArraySolution fromSolution(@Nonnull Solution solution) {
            int size = vars.size();
            Term[] values = new Term[size];
            for (int i = 0; i < size; i++)
                values[i] = solution.get(vars.get(i));
            return new ArraySolution(vars, values);
        }

        @CheckReturnValue
        public @Nonnull ArraySolution fromSolutions(@Nonnull Solution... solutions) {
            int size = vars.size();
            Term[] values = new Term[size];
            for (int i = 0; i < size; i++) {
                String var = vars.get(i);
                for (Solution solution : solutions) {
                    Term term = solution.get(var);
                    if (term != null) {
                        values[i] = term;
                        break;
                    }
                }
            }
            return new ArraySolution(vars, values);
        }
    }
}
