package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh;

import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface R2RMLView extends LogicalTable {
    @Nonnull String getSql();
    @Nullable Resource getSqlVersion();
}
