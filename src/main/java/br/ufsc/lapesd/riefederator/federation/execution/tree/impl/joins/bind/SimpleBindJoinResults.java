package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class SimpleBindJoinResults implements Results {
    public static final @Nonnull Logger logger = LoggerFactory.getLogger(SimpleBindJoinResults.class);

    private final @Nonnull PlanExecutor planExecutor;
    private final @Nonnull Results smaller;
    private final @Nonnull PlanNode rightTree;
    private Results rightResults = null;
    private final @Nonnull Collection<String> joinVars;
    private final @Nonnull Set<String> resultVars;
    private Solution leftSolution = null, next = null;

    public static class Factory implements BindJoinResultsFactory {
        private final @Nonnull Provider<PlanExecutor> planExecutorProvider;

        @Inject
        public Factory(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
            this.planExecutorProvider = planExecutorProvider;
        }

        @Override
        public @Nonnull Results createResults(@Nonnull Results smaller, @Nonnull PlanNode rightTree,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            PlanExecutor executor = planExecutorProvider.get();
            return new SimpleBindJoinResults(executor, smaller, rightTree, joinVars, resultVars);
        }
    }


    public SimpleBindJoinResults(@Nonnull PlanExecutor planExecutor, @Nonnull Results smaller,
                                 @Nonnull PlanNode rightTree, @Nonnull Collection<String> joinVars,
                                 @Nonnull Collection<String> resultVars) {
        Preconditions.checkArgument(rightTree.getResultVars().containsAll(joinVars),
                "There are joinVars missing on rightTree");
        this.planExecutor = planExecutor;
        this.smaller = smaller;
        this.rightTree = rightTree;
        this.joinVars = joinVars;
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : new HashSet<>(resultVars);
    }

    @Override
    public int getReadyCount() {
        return next != null ? 1 : 0;
    }

    private void advance() {
        assert next == null;

        while (rightResults == null || !rightResults.hasNext()) {
            if (!smaller.hasNext())
                return;
            if (rightResults != null) {
                try {
                    rightResults.close();
                } catch (ResultsCloseException e) {
                    logger.error("Problem closing rightResults of bound plan tree", e);
                }
            }
            leftSolution = smaller.next();
            PlanNode bound = bind(rightTree, leftSolution);
            rightResults = planExecutor.executeNode(bound);
        }
        Solution rightSolution = rightResults.next();
        MapSolution.Builder builder = MapSolution.builder();
        for (String name : resultVars) {
            Term term = leftSolution.get(name, rightSolution.get(name));
            if (term != null)
                builder.put(name, term);
        }
        next = builder.build();
    }

    private @Nonnull PlanNode bind(@Nonnull PlanNode node, @Nonnull Solution values) {
        MapSolution.Builder builder = MapSolution.builder();
        for (String name : joinVars) {
            Term term = values.get(name);
            if (term == null)
                logger.warn("Left Solution is missing join variable {}", name);
            else
                builder.put(name, term);
        }
        return node.createBound(builder.build());
    }

    @Override
    public boolean hasNext() {
        if (next == null)
            advance();
        return next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        assert next != null;
        Solution solution = next;
        next = null;
        return solution;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return smaller.getCardinality();
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return resultVars;
    }

    @Override
    public void close() throws ResultsCloseException {
        try {
            smaller.close();
        } finally {
            if (rightResults != null)
                rightResults.close();
        }
    }
}
