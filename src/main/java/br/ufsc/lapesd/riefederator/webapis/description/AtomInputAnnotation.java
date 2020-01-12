package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;

import javax.annotation.Nonnull;

/**
 * An {@link AtomAnnotation} whose {@link AtomAnnotation#isInput()} is always <code>true</code>.
 */
public class AtomInputAnnotation extends AtomAnnotation {
    public AtomInputAnnotation(@Nonnull Atom atom, boolean required) {
        super(atom, true, required);
    }

    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom) {
        return new AtomInputAnnotation(atom, false);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom) {
        return new AtomInputAnnotation(atom, true);
    }
}
