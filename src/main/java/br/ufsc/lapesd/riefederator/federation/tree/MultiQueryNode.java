package br.ufsc.lapesd.riefederator.federation.tree;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

/**
 * This node represents multiple independent queries that output the same projection.
 *
 * There is no need to perform duplicate removals at this stage (hence it is not a Union).
 */
public class MultiQueryNode extends PlanNode {
    public static class Builder {
        private List<PlanNode> list = new ArrayList<>();
        private Set<String> resultVars = null;
        private boolean project = false;
        private boolean intersect = false;

        public @Nonnull Builder add(@Nonnull PlanNode node) {
            list.add(node);
            return this;
        }

        public @Nonnull Builder addAll(@Nonnull Collection<? extends PlanNode> collection) {
            collection.forEach(this::add);
            return this;
        }

        public @Nonnull Builder setResultVarsNoProjection(@Nonnull Collection<String> varNames) {
            resultVars = varNames instanceof Set? (Set<String>)varNames : new HashSet<>(varNames);
            project = false;
            return this;
        }

        public @Nonnull Builder intersect() {
            intersect = true;
            project = true;
            return this;
        }

        public @Nonnull Builder allVars() {
            intersect = false;
            return this;
        }

        public @Nonnull
        MultiQueryNode build() {
            if (resultVars == null) {
                if (intersect) {
                    Iterator<PlanNode> i = list.iterator();
                    resultVars = new HashSet<>(i.hasNext() ? i.next().getResultVars() : emptySet());
                    while (i.hasNext()) {
                        Set<String> set = i.next().getResultVars();
                        project |= resultVars.retainAll(set);
                        if (!project && !resultVars.containsAll(set))
                            project = true;
                    }
                } else {
                    project = false;
                    resultVars = list.stream().flatMap(n -> n.getResultVars().stream())
                                              .collect(toSet());
                }
            } else if (MultiQueryNode.class.desiredAssertionStatus()) {
                Set<String> all = list.stream().flatMap(n -> n.getResultVars().stream())
                                               .collect(toSet());
                Preconditions.checkArgument(all.containsAll(resultVars),
                        "There are extra result vars");
                Preconditions.checkArgument(intersect || project == !resultVars.equals(all),
                        "Mismatch between project and resultVars");
            }
            return new MultiQueryNode(list, resultVars, project);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    protected MultiQueryNode(@Nonnull List<PlanNode> children, @Nonnull Collection<String> resultVars,
                             boolean projecting) {
        super(resultVars, projecting, children);
    }

    @Override
    protected @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        if (isProjecting())
            b.append(getPiWithNames()).append('(');
        for (PlanNode child : getChildren())
            child.toString(b).append(" + ");
        b.setLength(b.length()-3);
        if (isProjecting())
            b.append(')');
        return b;
    }
}
