package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RRTemplate;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface TermMap extends RRResource {
    @Nullable RDFNode getConstant();
    @Nullable String getColumn();
    @Nullable RRTemplate getTemplate();
    @Nonnull TermType getTermType();
    @Nullable String getLanguage();
    @Nullable Resource getDatatype();
}
