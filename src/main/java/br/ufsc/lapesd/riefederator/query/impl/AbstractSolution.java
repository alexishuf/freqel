package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.query.Solution;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class AbstractSolution implements Solution {

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        forEach((name, term) -> builder.append(name).append(term));
        return builder.toHashCode();
    }

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
