package br.ufsc.lapesd.freqel.description.molecules.annotations;

import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.annotations.InputAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Immutable
public class AtomInputAnnotation extends AtomAnnotation implements InputAnnotation {
    private final boolean required, missingInResult;
    private final @Nullable Term overrideValue;
    private final @Nonnull String inputName;

    /* --- --- --- Constructor & static factory methods  --- --- --- */

    public AtomInputAnnotation(@Nonnull Atom atom, boolean required, boolean missingInResult,
                               @Nonnull String inputName, @Nullable Term overrideValue) {
        super(atom);
        this.required = required;
        this.missingInResult = missingInResult;
        this.inputName = inputName;
        this.overrideValue = overrideValue;
    }

    public static class Builder {
        boolean required, missingInResult;
        @Nonnull Atom atom;
        @Nonnull String inputName;
        @Nullable Term overrideValue;

        public Builder(boolean required, @Nonnull Atom atom, @Nonnull String inputName) {
            this.required = required;
            this.atom = atom;
            this.inputName = inputName;
        }

        public @Nonnull Builder override(@Nullable Term overrideValue) {
            this.overrideValue = overrideValue;
            return this;
        }

        public @Nonnull Builder missingInResult() {
            return missingInResult(true);
        }

        public @Nonnull Builder missingInResult(boolean value) {
            this.missingInResult = value;
            return this;
        }

        public @Nonnull AtomInputAnnotation get() {
            return new AtomInputAnnotation(atom, required, missingInResult,
                                           inputName, overrideValue);
        }
    }

    public static @Nonnull Builder builder(boolean required, @Nonnull Atom atom,
                                           @Nonnull String inputName) {
        return new Builder(required, atom, inputName);
    }

    public static @Nonnull Builder asOptional(@Nonnull Atom atom, @Nonnull String inputName) {
        return new Builder(false, atom, inputName);
    }

    public static @Nonnull Builder asRequired(@Nonnull Atom atom, @Nonnull String inputName) {
        return new Builder(true, atom, inputName);
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

    @Override
    public boolean isMissingInResult() {
        return missingInResult;
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
