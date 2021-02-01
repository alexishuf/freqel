package br.ufsc.lapesd.freqel.jena.query.modifiers.filter;

import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.function.FunctionEnvBase;
import org.apache.jena.sparql.util.Context;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.toJenaNode;

@NotThreadSafe
public class JenaSPARQLFilterExecutor implements SPARQLFilterExecutor {
    private static final @Nonnull ExecutionContext defaultExecutionContext = new ExecutionContext();

    private final @Nonnull BindingWrapper tmp = new BindingWrapper();

    @Override public boolean evaluate(@Nullable SPARQLFilter filter, @Nonnull Solution solution) {
        if (filter == null)  return true; //a null filter is a non-filter
        tmp.setSolution(solution); //avoid the cost of new
        Expr expr = (JenaSPARQLFilter.build(filter)).getJenaExpr();
        return expr.isSatisfied(tmp, defaultExecutionContext.functionEnv);
    }

    private static class BindingWrapper implements Binding {
        private @Nullable Solution solution;

        public @Nonnull BindingWrapper setSolution(@Nonnull Solution solution) {
            this.solution = solution;
            return this;
        }

        @Override public Iterator<Var> vars() {
            if (solution == null) throw new IllegalStateException("No solution bound!");
            return null;
        }

        @Override public boolean contains(Var var) {
            if (solution == null) throw new IllegalStateException("No solution bound!");
            return false;
        }

        @Override public Node get(Var var) {
            return solution == null ? null : toJenaNode(solution.get(var.getVarName()));
        }

        @Override public int size() {
            return solution == null ? 0 : solution.getVarNames().size();
        }

        @Override public boolean isEmpty() {
            return solution == null || solution.isEmpty();
        }
    }

    private static class ExecutionContext {
        DatasetGraph dsg;
        Context context;
        FunctionEnvBase functionEnv;

        public ExecutionContext() {
            dsg = DatasetFactory.create().asDatasetGraph();
            context = Context.setupContextForDataset(ARQ.getContext(), dsg);
            functionEnv = new FunctionEnvBase(context, dsg.getDefaultGraph(), dsg);
        }
    }
}
