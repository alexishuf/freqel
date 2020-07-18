package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;

public interface JoinCondition extends RRResource {
    @Nonnull String getChild();
    @Nonnull String getParent();
}
