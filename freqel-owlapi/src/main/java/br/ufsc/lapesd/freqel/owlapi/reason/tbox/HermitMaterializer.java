package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import org.semanticweb.HermiT.ReasonerFactory;

public class HermitMaterializer extends OWLAPITBoxMaterializer {
    public HermitMaterializer() {
        super(new ReasonerFactory());
    }
}
