package br.ufsc.lapesd.freqel.query.results;

import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MutableSolution extends Solution {
    /**
     * Sets a value of a solution.
     * @param varName The variable name to change value
     * @param value The new value
     * @throws IllegalArgumentException if varName is not known by this solution and the
     *                                  implementation does not allow introduction of new variables
     * @return The old value, which may be null if non-existatn
     */
    @CanIgnoreReturnValue @Nullable Term set(@Nonnull String varName, @Nullable Term value);

    /**
     * Erases all values in this solution.
     *
     * If the implementation has fixed variables, the variables remain valid for future
     * {@link MutableSolution#set(String, Term)} calls.
     *
     * @return this {@link MutableSolution}
     */
    @CanIgnoreReturnValue @Contract(" -> this") @Nonnull MutableSolution clear();

    /**
     * Creates a copy of the {@link MutableSolution}
     */
    @CheckReturnValue @Nonnull MutableSolution copy();
}
