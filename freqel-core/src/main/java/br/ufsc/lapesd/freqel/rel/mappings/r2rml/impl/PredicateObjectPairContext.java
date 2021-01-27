package br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.ObjectMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.PredicateMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;

import javax.annotation.Nonnull;
import java.util.Objects;

public class PredicateObjectPairContext {
    private @Nonnull final TermContext pCtx, oCtx;

    public PredicateObjectPairContext(@Nonnull PredicateMap pm, @Nonnull ObjectMap om) {
        pCtx = new TermContext(pm);
        oCtx = new TermContext(om);
        if (pCtx.getRoot().getTermType() != TermType.IRI) {
            throw new InvalidRRException(pCtx.getRoot(), RR.termType,
                                         "Predicate map has non-IRI term type");
        }
    }

    public @Nonnull TermContext getPredicate() {
        return pCtx;
    }

    public @Nonnull TermContext getObject() {
        return oCtx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PredicateObjectPairContext)) return false;
        PredicateObjectPairContext that = (PredicateObjectPairContext) o;
        return pCtx.equals(that.pCtx) &&
                oCtx.equals(that.oCtx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pCtx, oCtx);
    }
}
