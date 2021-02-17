package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.GlobalContextAnnotation;

import javax.annotation.Nonnull;

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
    ALTERNATIVES;

    public @Nonnull MatchReasoning ifRequested(@Nonnull CQuery query) {
        if (query.getModifiers().reasoning() != null)
            return this;
        GlobalContextAnnotation a = query.getQueryAnnotation(GlobalContextAnnotation.class);
        if (a != null) {
            Op root = a.get(GlobalContextAnnotation.USER_QUERY, Op.class);
            if (root != null && root.modifiers().reasoning() != null)
                return this;
        }
        return NONE;
    }

    public @Nonnull MatchReasoning ifRequested(@Nonnull Op op) {
        if (op instanceof QueryOp)
            return ifRequested(((QueryOp) op).getQuery());
        return op.modifiers().reasoning() != null ? this : NONE;
    }
}
