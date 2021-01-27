package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;

public interface JoinCondition extends RRResource {
    @Nonnull String getChild();
    @Nonnull String getParent();
}
