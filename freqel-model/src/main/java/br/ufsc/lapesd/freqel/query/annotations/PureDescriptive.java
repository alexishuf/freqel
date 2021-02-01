package br.ufsc.lapesd.freqel.query.annotations;

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
