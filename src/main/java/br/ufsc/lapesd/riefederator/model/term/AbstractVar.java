package br.ufsc.lapesd.riefederator.model.term;

import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;

@Immutable
public abstract class AbstractVar implements Var {

    @Override
    @OverridingMethodsMustInvokeSuper
    public boolean equals(Object obj) {
        return (obj instanceof Var) && getName().equals(((Var)obj).getName());
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public int hashCode() {
        return getName().hashCode();
    }
}
