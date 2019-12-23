package br.ufsc.lapesd.riefederator.description.semantic;

import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public interface SemanticDescription extends Description {
    @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query);
}
