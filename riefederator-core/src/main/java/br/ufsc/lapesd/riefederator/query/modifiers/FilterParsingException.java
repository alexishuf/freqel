package br.ufsc.lapesd.riefederator.query.modifiers;

import javax.annotation.Nonnull;

public class FilterParsingException extends IllegalArgumentException {
    private final @Nonnull String filterExpression;

    public FilterParsingException(@Nonnull String filterExpression) {
        super("Invalid filter expression: "+filterExpression);
        this.filterExpression = filterExpression;
    }

    public FilterParsingException(@Nonnull String filterExpression, @Nonnull Throwable cause) {
        super("Invalid filter expression: "+filterExpression, cause);
        this.filterExpression = filterExpression;
    }

    /**
     * Get the inner filter expression (The "x" in "FILTER(x)").
     */
    public @Nonnull String getFilterExpression() {
        return filterExpression;
    }
}
