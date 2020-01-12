package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.tree.EmptyNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.impl.CollectionResults;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;

@Immutable
public class SimpleEmptyNodeExecutor implements EmptyNodeExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SimpleEmptyNodeExecutor.class);
    public static final @Nonnull SimpleEmptyNodeExecutor INSTANCE = new SimpleEmptyNodeExecutor();

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return EmptyNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) {
        if (node instanceof EmptyNode)
            return execute((EmptyNode)node);
        logger.warn("Received a {} will return empty result instead of throwing", node.getClass());
        return new CollectionResults(Collections.emptyList(), node.getResultVars());
    }

    @Override
    public @Nonnull Results execute(@Nonnull EmptyNode node) {
        return new CollectionResults(Collections.emptyList(), node.getResultVars());
    }
}
