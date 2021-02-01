package br.ufsc.lapesd.freqel.model.term;

import br.ufsc.lapesd.freqel.model.TermTypeException;
import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public interface Term {
    enum Type {
        URI, LITERAL, BLANK, VAR;

        static @Nonnull <T extends Term> Type fromClass(Class<T> cls) {
            if (br.ufsc.lapesd.freqel.model.term.URI.class.isAssignableFrom(cls))   return URI;
            if (Lit.class.isAssignableFrom(cls))   return LITERAL;
            if (Blank.class.isAssignableFrom(cls)) return BLANK;
            if (Var.class.isAssignableFrom(cls))       return VAR;
            throw new IllegalArgumentException("Cannot handle" + cls);
        }

    }
    Type getType();

    default boolean     isURI() { return getType() == Type.URI;    }
    default boolean isLiteral() { return getType() == Type.LITERAL;}
    default boolean   isBlank() { return getType() == Type.BLANK;  }
    default boolean     isVar() { return getType() == Type.VAR;    }
    /** A Term is ground iff it is not a variable nor a blank node. */
    default boolean  isGround() { return !isVar() && !isBlank();   }
    /** Implements {@link Res}, i.e., it is a blank node or an URI. */
    default boolean     isRes() { return this instanceof Res;      }

    default @Nonnull URI asURI() { return as(URI.class);  }
    default @Nonnull Lit asLiteral() { return as(Lit.class);  }
    default @Nonnull Blank asBlank() { return as(Blank.class);}
    default @Nonnull Var     asVar() { return as(Var.class);  }
    default @Nonnull <T extends Term> T as(Class<T> cls) {
        if (cls.isInstance(this)) {
            //noinspection unchecked
            return (T) this;
        }
        throw new TermTypeException(Type.fromClass(cls), this);
    }

    default <T> T as(Class<T> cls, @Nullable T fallback) {
        if (cls.isInstance(this)) {
            //noinspection unchecked
            return (T) this;
        }
        return fallback;
    }

    @Nonnull String toString(@Nonnull PrefixDict dict);
}
