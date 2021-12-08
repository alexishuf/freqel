package br.ufsc.lapesd.freqel.query.results;

import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

public abstract class AbstractResults implements Results {
    protected @Nonnull Set<String> varNames;
    protected @Nullable String name;
    private boolean optional = false;

    protected AbstractResults(@Nonnull Collection<String> varNames) {
        this.varNames = varNames instanceof IndexSet ? (Set<String>)varNames
                                                     : FullIndexSet.from(varNames);
    }

    @Override
    public int getReadyCount() {
        return 0;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public int getLimit() {
        return -1;
    }

    @Override
    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public @Nullable String getNodeName() {
        return name;
    }

    @Override
    public void setNodeName(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        int id = System.identityHashCode(this);
        String optSuffix = optional ? "[OPTIONAL]" : "";
        String suffix = name == null ? "" : " for " + name;
        return String.format("%s@%x%s%s", getClass().getSimpleName(), id, optSuffix, suffix);
    }
}
