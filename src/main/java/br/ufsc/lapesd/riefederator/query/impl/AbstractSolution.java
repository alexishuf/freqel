package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class AbstractSolution implements Solution {
    private static @LazyInit int hashCache = 0;

    @Override
    public int hashCode() {
        int local = hashCache;
        if (local == 0) {
            HashCodeBuilder builder = new HashCodeBuilder();
            forEach((name, term) -> builder.append(name).append(term));
            hashCache = local = builder.toHashCode();
        }
        return local;
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof Solution)) return false;
        Solution rhs = (Solution)obj;

        EqualsBuilder builder = new EqualsBuilder();
        forEach((name, term) -> builder.append(term, rhs.get(name)));
        rhs.forEach((name, term) -> builder.append(get(name), term));
        return builder.isEquals();
    }
}
