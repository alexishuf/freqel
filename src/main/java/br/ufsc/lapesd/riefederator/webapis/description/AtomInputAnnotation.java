package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Immutable
public class AtomInputAnnotation extends AtomAnnotation implements InputAnnotation {
    private final boolean required;
    private final @Nullable Term overrideValue;
    private final @Nonnull String inputName;

    /* --- --- --- Constructor & static factory methods  --- --- --- */

    public AtomInputAnnotation(@Nonnull Atom atom, boolean required, @Nonnull String inputName,
                               @Nullable Term overrideValue) {
        super(atom);
        this.required = required;
        this.inputName = inputName;
        this.overrideValue = overrideValue;
    }

    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom,
                                                          @Nonnull String inputName,
                                                          @Nullable Term overrideValue) {
        return new AtomInputAnnotation(atom, false, inputName, overrideValue);
    }
    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom,
                                                          @Nonnull String inputName) {
        return asOptional(atom, inputName, null);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom,
                                                          @Nonnull String inputName,
                                                          @Nullable Term overrideValue) {
        return new AtomInputAnnotation(atom, true, inputName, overrideValue);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom,
                                                          @Nonnull String inputName) {
        return asRequired(atom, inputName, null);
    }


    /* --- --- --- Getters --- --- --- */

    public boolean isOverride() {
        return overrideValue != null;
    }

    public @Nullable Term getOverrideValue() {
        return overrideValue;
    }

    public @Nonnull String getInputName() {
        return inputName;
    }

    @Override
    public boolean isInput() {
        return true;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    /* --- --- --- Object overloads --- --- --- */

    @Override
    public String toString() {
        return String.format("%s(%s%s)->%s",
                isRequired() ? "REQUIRED" : "OPTIONAL",
                getAtomName(),
                getOverrideValue() != null ? "="+getOverrideValue() : "",
                getInputName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomInputAnnotation)) return false;
        if (!super.equals(o)) return false;
        AtomInputAnnotation that = (AtomInputAnnotation) o;
        return isRequired() == that.isRequired() &&
                Objects.equals(getOverrideValue(), that.getOverrideValue()) &&
                getInputName().equals(that.getInputName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isRequired(), getOverrideValue(), getInputName());
    }
}
