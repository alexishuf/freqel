package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

public abstract class AbstractSolution implements Solution {
    protected @LazyInit int hashCache = 0;

    @Override
    public int hashCode() {
        int[] local = {hashCache};
        if (local[0] == 0) {
            local[0] = 17;
            Set<String> names = getVarNames();
            if (!(names instanceof SortedSet) && names.size() > 1)
                names = ArraySet.fromDistinct(names);
            for (String name : names) {
                Term term = get(name);
                if (term != null)
                    local[0] = (local[0] * 37 + name.hashCode()) * 37 + term.hashCode();
            }
            hashCache = local[0];
        }
        return local[0];
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof Solution)) return false;
        Solution rhs = (Solution)obj;
        if (hashCache != 0 && hashCache != rhs.hashCode())
            return false;
        Set<String> names = getVarNames(), smallerNames = rhs.getVarNames();
        if (smallerNames.size() > names.size())
            names = smallerNames;
        if (!names.containsAll(smallerNames))
            return false;
        for (String name : getVarNames()) {
            if (!Objects.equals(get(name), rhs.get(name)))
                return false;
        }
        return true;
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('@')
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append('{');
        forEach((name, value) -> b.append(name).append('=').append(value).append(", "));
        b.setLength(b.length()-2);
        return b.append('}').toString();
    }
}
