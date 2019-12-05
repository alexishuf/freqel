package br.ufsc.lapesd.riefederator.rdf.term;

import br.ufsc.lapesd.riefederator.rdf.term.parse.TermTypeException;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public interface Term {
    enum Type {
        URI, LITERAL, BLANK, VAR;

        static @Nonnull <T extends Term> Type fromClass(Class<T> cls) {
            if (br.ufsc.lapesd.riefederator.rdf.term.URI.class.isAssignableFrom(cls))   return URI;
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

    default @Nonnull
    URI asURI() { return as(URI.class);  }
    default @Nonnull
    Lit asLiteral() { return as(Lit.class);  }
    default @Nonnull
    Blank asBlank() { return as(Blank.class);}
    default @Nonnull Var           asVar() { return as(Var.class);      }

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

    default boolean accepts(@Nonnull Term other) {
        if (getType() != other.getType())
            return false;
        switch (getType()) {
            case URI:
                return asURI().getURI().equals(other.asURI().getURI());
            case LITERAL:
                Lit me = asLiteral(), him = other.asLiteral();
                return me.getLexicalForm().equals(him.getLexicalForm())
                        && me.getDatatype().equals(him.getDatatype());
            case BLANK:
                return asBlank().getId().equals(other.asBlank().getId());
            case VAR:
                return asVar().getName().equals(other.asVar().getName());
        }
        throw new IllegalStateException("Unknown type " + getType());
    }
}
