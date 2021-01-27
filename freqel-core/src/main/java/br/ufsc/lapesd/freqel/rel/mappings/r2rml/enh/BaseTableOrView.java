package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;

public interface BaseTableOrView extends LogicalTable {
    @Nonnull String getTableName();
}
