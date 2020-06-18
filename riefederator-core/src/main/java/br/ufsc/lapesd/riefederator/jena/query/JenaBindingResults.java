package br.ufsc.lapesd.riefederator.jena.query;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class JenaBindingResults implements Results {
    private final @Nullable QueryExecution execution;
    private final @Nonnull Set<String> varNames;
    private final @Nonnull ResultSet resultSet;
    private final JenaBindingSolution.Factory solutionFactory;
    private @Nullable String name;

    public JenaBindingResults(@Nonnull ResultSet resultSet, @Nullable QueryExecution execution,
                              @Nonnull Set<String> varNames) {
        this.resultSet = resultSet;
        this.execution = execution;
        this.varNames = varNames;
        solutionFactory = JenaBindingSolution.forVars(varNames);

    }

    public JenaBindingResults(@Nonnull ResultSet resultSet, @Nullable QueryExecution execution) {
        this(resultSet, execution, ArraySet.fromDistinct(resultSet.getResultVars()));
    }

    @Override
    public int getReadyCount() {
        return hasNext() ? 1 : 0;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return resultSet.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        return solutionFactory.apply(resultSet.nextBinding());
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public void setNodeName(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public void close() throws ResultsCloseException {
        if (execution != null) execution.close();
    }
}
