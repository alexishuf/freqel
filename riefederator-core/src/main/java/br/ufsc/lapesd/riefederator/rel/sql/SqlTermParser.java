package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.model.term.Term;
import org.apache.jena.rdf.model.RDFNode;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nullable;

public interface SqlTermParser {
    @Contract("null -> null ; !null -> !null")
    @Nullable Term parseTerm(@Nullable Object sqlObject);

    @Contract("null -> null ; !null -> !null")
    @Nullable RDFNode parseNode(@Nullable Object sqlObject);
}
