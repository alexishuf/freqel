package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.model.term.Term;
import org.apache.jena.rdf.model.RDFNode;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nullable;

public interface RelationalTermParser {
    @Contract("null -> null ; !null -> !null")
    @Nullable Term parseTerm(@Nullable Object sqlObject);

    @Contract("null -> null ; !null -> !null")
    @Nullable RDFNode parseNode(@Nullable Object sqlObject);
}
