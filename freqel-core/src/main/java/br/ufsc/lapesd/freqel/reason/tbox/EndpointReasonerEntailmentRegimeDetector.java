package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.reason.regimes.SourcedEntailmentRegime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Determines a value for {@link FreqelConfig.Key#ADVERTISED_REASONING} by looking up
 * configurations other than {@link FreqelConfig.Key#ADVERTISED_REASONING}.
 *
 * An implementation of this interface may return null if it has no conclusion to offer.
 * Multiple implementations are expected to register on SPI and ideally only one should
 * return non-null.
 */
public interface EndpointReasonerEntailmentRegimeDetector {
    @Nullable SourcedEntailmentRegime get(@Nonnull FreqelConfig config);
}
