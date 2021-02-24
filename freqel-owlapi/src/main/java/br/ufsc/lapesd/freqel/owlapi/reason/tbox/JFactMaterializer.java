package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import uk.ac.manchester.cs.jfact.JFactFactory;

public class JFactMaterializer extends OWLAPITBoxMaterializer {
    public JFactMaterializer() {
        super(new JFactFactory(), true);
    }
}
