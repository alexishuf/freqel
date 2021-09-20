package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.reason.regimes.SourcedEntailmentRegime;
import br.ufsc.lapesd.freqel.reason.tbox.endpoint.HeuristicEndpointReasoner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.CROSS_SOURCE;
import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.SINGLE_SOURCE_ABOX;
import static br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes.RDFS;
import static br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes.SIMPLE;

public class DefaultEndpointReasonerEntailmentRegimeDetector implements EndpointReasonerEntailmentRegimeDetector {
    private final String NO_NAME = NoEndpointReasoner.class.getName();
    private final String HEURISTIC_NAME = HeuristicEndpointReasoner.class.getName();

    @Override public @Nullable SourcedEntailmentRegime get(@Nonnull FreqelConfig config) {
        String name = config.get(FreqelConfig.Key.ENDPOINT_REASONER, String.class);
        if (name == null || name.equals(NO_NAME) || NO_NAME.endsWith(name))
            return new SourcedEntailmentRegime(CROSS_SOURCE, SIMPLE);
        if (name.equals(HEURISTIC_NAME) || HEURISTIC_NAME.endsWith(name))
            return new SourcedEntailmentRegime(SINGLE_SOURCE_ABOX, RDFS);
        return null;
    }
}
