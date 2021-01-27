package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.federation.execution.tree.EmptyOpExecutor;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsUtils;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;

@Immutable
public class SimpleEmptyOpExecutor implements EmptyOpExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleEmptyOpExecutor.class);
    public static final @Nonnull SimpleEmptyOpExecutor INSTANCE = new SimpleEmptyOpExecutor();

    @Override
    public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return EmptyOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) {
        if (node instanceof EmptyOp)
            return execute((EmptyOp)node);
        logger.warn("Received a {} will return empty result instead of throwing", node.getClass());
        return new CollectionResults(Collections.emptyList(), node.getResultVars());
    }

    @Override
    public @Nonnull Results execute(@Nonnull EmptyOp node) {
        Results r = new CollectionResults(Collections.emptyList(), node.getResultVars());
        return ResultsUtils.applyModifiers(r, node.modifiers());
    }
}
