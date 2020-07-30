package br.ufsc.lapesd.riefederator.query.results;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.BiConsumer;

public interface Solution {
    /**
     * Gets the value bound to the given name.
     *
     * @param varName Var name to get the bound Term
     * @param fallback what to return if varName is not bound
     * @return Term bound to varName or fallback
     */
    @Contract("_, !null -> !null")
    Term get(@Nonnull String varName, Term fallback);

    /**
     * Apply a function over all name -> term pairs in this {@link Solution}.
     */
    void forEach(@Nonnull BiConsumer<String, Term> consumer);

    default @Nullable Term get(@Nonnull String varName) {
        return get(varName, null);
    }
    default @Nullable Term get(@Nonnull Var var) {
        return get(var.getName());
    }
    default boolean has(@Nonnull String varName) {
        return get(varName) != null;
    }

    default boolean isEmpty() {
        if (getVarNames().isEmpty())
            return true;
        boolean[] empty = {true};
        forEach((n, t) -> empty[0] &= t == null);
        return empty[0];
    }

    /**
     * Gets all var names for which {@link Solution#has(java.lang.String)} returns true.
     */
    @Nonnull Set<String> getVarNames();
}
