package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

public class SemanticAskDescription  extends AskDescription implements SemanticDescription {
    private @Nonnull final TBox tBox;

    public SemanticAskDescription(@Nonnull TPEndpoint endpoint, @Nonnull TBox tBox, int cacheSize) {
        super(endpoint, cacheSize);
        this.tBox = tBox;
    }

    public SemanticAskDescription(@Nonnull TPEndpoint endpoint, @Nonnull TBox tBox) {
        super(endpoint);
        this.tBox = tBox;
    }

    private @Nullable Boolean
    matchAlternatives(boolean onlyCache, @Nonnull MatchReasoning mode, @Nonnull Triple triple,
                      @Nonnull Triple.Position position, @Nonnull Iterator<Term> alternatives) {
        Boolean fallback = onlyCache ? null : false;
        while (alternatives.hasNext()) {
            Boolean m = super.match(triple.with(position, alternatives.next()), mode, onlyCache);
            if      (Boolean.TRUE.equals(m))  return true;
            else if (Boolean.FALSE.equals(m)) fallback = false;
        }
        return fallback;
    }

    @Override
    protected @Nullable Boolean match(@Nonnull Triple triple, @Nonnull MatchReasoning reasoning,
                                      boolean onlyCache) {
        if (reasoning != MatchReasoning.TRANSPARENT)
            return super.match(triple, reasoning, onlyCache);
        Triple sanitized = sanitize(triple);
        if (sanitized.getPredicate().equals(V.RDF.type)) {
            if (!sanitized.getObject().isGround())
                super.match(sanitized, reasoning, onlyCache);
            return matchAlternatives(onlyCache, reasoning, triple, Triple.Position.OBJ,
                                     tBox.withSubClasses(triple.getObject()).iterator());
        } else if (sanitized.getPredicate().isGround()) {
            return matchAlternatives(onlyCache, reasoning, triple, Triple.Position.PRED,
                                     tBox.withSubProperties(triple.getPredicate()).iterator());
        }
        return super.match(sanitized, reasoning, onlyCache);
    }

    @Override public boolean supports(@Nonnull MatchReasoning mode) {
        return MatchReasoning.NONE.equals(mode) || MatchReasoning.TRANSPARENT.equals(mode);
    }

    @Override public @Nonnull String toString() {
        return "SemanticAskDescription("+endpoint+")";
    }
}
