package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.term.Term;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
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
    default boolean has(@Nonnull String varName) {
        return get(varName) != null;
    }

    /**
     * Gets all var names for which {@link Solution#has(java.lang.String)} returns true.
     */
    @Nonnull Collection<String> getVarNames();
}
