package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;

public interface SemanticDescription extends Description {
    @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query);
}
