package br.ufsc.lapesd.freqel.reason.regimes.subsumption;

import br.ufsc.lapesd.freqel.reason.regimes.EntailmentRegime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface EntailmentRegimeSubsumptionChecker {
    /**
     * Tests whether any entailment generated by subsumed can also be generated by subsumer.
     *
     * @param subsumer the supposedly more powerful regime
     * @param subsumed the supposedly equivalent or weaker regime
     * @return true iff subsumer knowingly subsumes subsumed, false if it does not and
     *         null if unknown
     */
    @Nullable Boolean subsumes(@Nonnull EntailmentRegime subsumer,
                               @Nonnull EntailmentRegime subsumed);
}
