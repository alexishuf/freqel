package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.query.TripleAnnotation;
import com.google.errorprone.annotations.Immutable;

@Immutable
public class PureDescriptive implements TripleAnnotation {
    public static final PureDescriptive INSTANCE = new PureDescriptive();

    @Override
    public String toString() {
        return "PureDescriptiveAnnotation";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PureDescriptive;
    }

    @Override
    public int hashCode() {
        return 37;
    }
}
