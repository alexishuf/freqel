package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;

public class SubPathAggregation {
    private @Nonnull JoinGraph graph;
    private @Nonnull List<JoinPath> joinPaths;

    private SubPathAggregation(@Nonnull JoinGraph graph, @Nonnull List<JoinPath> joinPaths) {
        this.graph = graph;
        this.joinPaths = joinPaths;
    }

    public static @Nonnull
    SubPathAggregation aggregate(@Nonnull JoinGraph g, @Nonnull Collection<JoinPath> paths,
                                 @Nonnull JoinOrderPlanner joinOrderPlanner) {
        if (paths.isEmpty()) {
            return new SubPathAggregation(new JoinGraph(), Collections.emptyList());
        }
        List<JoinPath> pathsList = paths instanceof List ? (List<JoinPath>)paths
                                                         : new ArrayList<>(paths);
        State state = new State(g);
        int i = -1, size = pathsList.size();
        for (JoinPath path : pathsList) {
            ++i;
            for (int j = i+1; j < size; j++)
                state.processPair(path, pathsList.get(j));
        }

        state.planComponents(joinOrderPlanner);
        JoinGraph reducedGraph = state.createReducedJoinGraph(pathsList);
        List<JoinPath> reducedPaths = new ArrayList<>(pathsList.size());
        for (JoinPath path : pathsList) {
            reducedPaths.add(state.reducePath(path));
        }
        return new SubPathAggregation(reducedGraph, reducedPaths);
    }

    public @Nonnull JoinGraph getGraph() {
        return graph;
    }

    public @Nonnull List<JoinPath> getJoinPaths() {
        return joinPaths;
    }

    @VisibleForTesting
    static class PlannedComponent {
        IndexedSubset<PlanNode> component;
        PlanNode node;

        public PlannedComponent(IndexedSubset<PlanNode> component, PlanNode node) {
            this.component = component;
            this.node = node;
        }
    }

    @VisibleForTesting
    static class State {
        private @Nonnull JoinGraph graph;
        private @Nonnull List<IndexedSubset<PlanNode>> components;
        private List<PlannedComponent> planned;
        private JoinGraph reducedGraph;

        public State(@Nonnull JoinGraph graph) {
            this.graph = graph;
            this.components = new ArrayList<>(graph.size());
        }

        @VisibleForTesting
        @Nonnull List<IndexedSubset<PlanNode>> getComponents() {
            return components;
        }

        @VisibleForTesting
        @Nonnull List<PlannedComponent> getPlanned() {
            return planned;
        }

        public void planComponents(@Nonnull JoinOrderPlanner planner) {
            assert components.stream().noneMatch(Objects::isNull);
            assert components.stream().noneMatch(Set::isEmpty);
            checkDisjointness();

            planned = new ArrayList<>(components.size());
            for (IndexedSubset<PlanNode> component : components) {
                if (component.size() == 1)
                    planned.add(new PlannedComponent(component, component.iterator().next()));
                else
                    planned.add(new PlannedComponent(component, planner.plan(graph, component)));
            }
        }

        public @Nonnull JoinGraph createReducedJoinGraph(@Nonnull Collection<JoinPath> paths) {
            Preconditions.checkState(planned != null, "Call planComponents() before!");
            Preconditions.checkState(planned.size() == components.size());

            IndexedSubset<PlanNode> visited = graph.getNodes().emptySubset();
            List<PlanNode> nodes = new ArrayList<>(graph.size());
            for (PlannedComponent pc : planned) {
                nodes.add(pc.node);
                visited.addAll(pc.component);
            }
            for (JoinPath path : paths) {
                IndexedSubset<PlanNode> novel = path.getNodes().createDifference(visited);
                nodes.addAll(novel);
                visited.addAll(novel);
            }

            reducedGraph = new JoinGraph(IndexedSet.fromDistinct(nodes));
            return reducedGraph;
        }

        public @Nonnull JoinPath reducePath(@Nonnull JoinPath path) {
            if (path.isWhole())
                return path;
            IndexedSubset<PlanNode> nodes = reducedGraph.getNodes().emptySubset();
            IndexedSubset<PlanNode> pending = path.getNodes().copy();
            boolean needsRebuild = false;
            for (PlannedComponent pc : planned) {
                if (path.getNodes().containsAll(pc.component)) {
                    nodes.add(pc.node);
                    pending.removeAll(pc.component);
                    needsRebuild |= pc.component.size() > 1;
                }
            }
            if (needsRebuild) {
                nodes.addAll(pending);
                JoinPath reducedPath = JoinPath.findPath(reducedGraph, nodes);
                assert reducedPath != null;
                return reducedPath;
            }
            return path; // no components or only singleton components
        }

        public void processPair(@Nonnull JoinPath left, @Nonnull JoinPath right) {
            assert left.getNodes().getParent().containsAll(right.getNodes());
            assert right.getNodes().getParent().containsAll(left.getNodes());
            IndexedSubset<PlanNode> common = left.getNodes().createIntersection(right.getNodes());
            IndexedSubset<PlanNode> visited = common.copy();
            visited.clear();
            ArrayDeque<PlanNode> stack = new ArrayDeque<>();
            for (PlanNode start : common) {
                if (visited.contains(start))
                    continue;
                stack.push(start);
                IndexedSubset<PlanNode> component = common.copy();
                component.clear();
                while (!stack.isEmpty()) {
                    PlanNode node = stack.pop();
                    if (component.add(node)) {
                        graph.forEachNeighbor(node, (i, n) -> {
                            if (common.contains(n)) stack.push(n);
                        });
                    }
                }
                visited.union(component);
                store(component);
            }
        }

        public void store(@Nonnull IndexedSubset<PlanNode> novel) {
            checkJoinConnected(novel);
            List<IndexedSubset<PlanNode>> dismembered = new ArrayList<>();
            for (IndexedSubset<PlanNode> c : components) {
                IndexedSubset<PlanNode> common = c.createIntersection(novel);
                if (!common.isEmpty()) {
                    if (common.size() != c.size()) {
                        c.removeAll(common);
                        dismembered.add(common);
                    }
                    novel.removeAll(common);
                }
            }
            components.addAll(dismembered);
            if (!novel.isEmpty())
                components.add(novel);
            checkDisjointness();
        }

        private void checkJoinConnected(@Nonnull IndexedSubset<PlanNode> component) {
            if (!SubPathAggregation.class.desiredAssertionStatus())
                return; //check is expensive
            if (component.isEmpty())
                return; //empty is always join-connected
            IndexedSubset<PlanNode> visited = component.copy();
            visited.clear();
            ArrayDeque<PlanNode> stack = new ArrayDeque<>(component.size());
            stack.push(component.iterator().next());
            while (!stack.isEmpty()) {
                PlanNode node = stack.pop();
                if (visited.add(node)) {
                    graph.forEachNeighbor(node, (i, n) -> {
                        if (component.contains(n))
                            stack.push(n);
                    });
                }
            }
            assert visited.size() <= component.size();
            if (!visited.equals(component)) {
                IndexedSubset<PlanNode> missing = component.copy();
                missing.removeAll(visited);
                throw new IllegalArgumentException("component "+component+" is disconnected. "+
                        missing+" are not reachable from the first member");
            }
        }

        private void checkDisjointness() {
            if (State.class.desiredAssertionStatus()) {
                for (int i = 0; i < components.size(); i++) {
                    for (int j = i+1; j < components.size(); j++) {
                        IndexedSubset<PlanNode> bad;
                        bad = components.get(i).createIntersection(components.get(j));
                        assert bad.isEmpty() : i+"-th and "+j+"-th components intersect: "+bad;
                    }
                }
            }
        }
    }
}
