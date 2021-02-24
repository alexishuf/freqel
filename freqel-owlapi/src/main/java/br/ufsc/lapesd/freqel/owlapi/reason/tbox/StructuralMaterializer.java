package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;

public class StructuralMaterializer extends OWLAPITBoxMaterializer {
    public StructuralMaterializer() {
        super(new StructuralReasonerFactory());
    }
}
