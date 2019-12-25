package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Capability;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;
import java.util.Collections;

import static java.util.stream.Collectors.toSet;

public class EmptyEndpoint implements CQEndpoint {
    @Override
    public @Nonnull
    Results query(@Nonnull CQuery query) {
        return new CollectionResults(Collections.emptyList(),
                query.streamTerms(Var.class).map(Var::getName).collect(toSet()));
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        return true;
    }
}
