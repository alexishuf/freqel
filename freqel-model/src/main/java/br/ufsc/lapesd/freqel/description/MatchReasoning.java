package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.description.semantic.SemanticCQueryMatch;

public enum MatchReasoning {
    /**
     * Do not perform reasoning
     */
    NONE,
    /**
     * Perform reasoning to match queries, but match the query as provided. Successful usage
     * of this mode requires that at execution time the input query be rewritten or undergo
     * additional processing in order to send a query for which the source has actual results.
     */
    TRANSPARENT,
    /**
     * Instead of producing a {@link CQueryMatch}, produce a {@link SemanticCQueryMatch} that
     * contains all alternative rewritings for matched triples and exclusive groups.
     */
    ALTERNATIVES
}
