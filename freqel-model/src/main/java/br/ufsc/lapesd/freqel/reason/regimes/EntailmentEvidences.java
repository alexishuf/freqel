package br.ufsc.lapesd.freqel.reason.regimes;

import javax.annotation.Nonnull;

public enum EntailmentEvidences {
    /**
     * All evidences for an entailed triple come from a single source.
     */
    SINGLE_SOURCE,
    /**
     * All evidences triples which lie in the ABox required to entail a
     * given triple, come from a single source, whereas TBox-originated
     * evidences may come from multiple sources
     */
    SINGLE_SOURCE_ABOX,
    /**
     * Evidences for an entailed triple can originate from multiple sources.
     */
    CROSS_SOURCE;

    public boolean subsumes(@Nonnull EntailmentEvidences other) {
        return ordinal() >= other.ordinal();
    }
}
