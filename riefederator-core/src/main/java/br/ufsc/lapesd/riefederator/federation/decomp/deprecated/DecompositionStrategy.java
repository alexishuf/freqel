package br.ufsc.lapesd.riefederator.federation.decomp.deprecated;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface DecompositionStrategy {
    void addSource(@Nonnull Source source);
    @Nonnull Collection<Op> decomposeIntoLeaves(@Nonnull CQuery query);
    @Nonnull Op decompose(@Nonnull CQuery query);
    @Nonnull Collection<Source> getSources();
}
