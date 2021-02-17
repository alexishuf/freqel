package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.jena.ModelUtils;
import br.ufsc.lapesd.freqel.jena.model.term.JenaTerm;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.Blank;
import br.ufsc.lapesd.freqel.model.term.Res;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import com.google.common.base.Preconditions;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A Simple reasoner that only processes the transitivity of subClassOf/subPropertyOf properties.
 */
public class TransitiveClosureTBoxMaterializer implements TBoxMaterializer {
    private static final Logger logger = LoggerFactory.getLogger(TransitiveClosureTBoxMaterializer.class);
    private @Nullable Model model;
    private boolean warnedEmpty = false;
    private @Nullable TPEndpoint endpoint = null;
    private @Nullable String name;

    public TransitiveClosureTBoxMaterializer() { }

    public TransitiveClosureTBoxMaterializer(@Nonnull TBoxSpec spec) throws TBoxLoadException {
        load(spec);
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    private void warnEmpty() {
        if (warnedEmpty)
            return;
        warnedEmpty = true;
        logger.info("{} has not been load()ed will answer over an empty TBox", this);
    }

    @Override public @Nullable TPEndpoint getEndpoint() {
        if (endpoint != null)
            return endpoint;
        if (model == null)
            return endpoint = new EmptyEndpoint();
        materialize();
        return endpoint = ARQEndpoint.forModel(model, this.toString());
    }

    private void materialize() {
        assert model != null;
        Graph outGraph = GraphFactory.createDefaultGraph();
        model.getGraph().find().forEachRemaining(outGraph::add);
        materialize(outGraph, model.getGraph(), RDFS.subClassOf.asNode());
        materialize(outGraph, model.getGraph(), RDFS.subPropertyOf.asNode());
        model.close();
        model = ModelFactory.createModelForGraph(outGraph);
    }

    private
    void materialize(@Nonnull Graph outGraph, @Nonnull Graph inGraph, @Nonnull Node edge) {
        IndexSet<Node> nodes = new FullIndexSet<>(Math.max(256, inGraph.size()/4));
        for (Iterator<Triple> it = inGraph.find(null, edge, null); it.hasNext(); ) {
            Triple triple = it.next();
            nodes.add(triple.getSubject());
            nodes.add(triple.getObject());
        }
        nodes.stream().parallel().forEach(n -> materialize(outGraph, inGraph, nodes, n, edge));
    }

    private void materialize(@Nonnull Graph outGraph, @Nonnull Graph inGraph, IndexSet<Node> nodes,
                             @Nonnull Node node, @Nonnull Node edge) {
        Set<Node> visited = nodes.emptySubset();
        ArrayDeque<Node> stack = new ArrayDeque<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            Node next = stack.pop();
            if (visited.add(next))
                inGraph.find(next, edge, null).forEachRemaining(t -> stack.push(t.getObject()));
        }
        synchronized (this) {
            for (Node next : visited)
                outGraph.add(new Triple(node, edge, next));
        }
    }

    @Override
    public void load(@Nonnull TBoxSpec sources) {
        model = sources.loadModel();
        if (endpoint != null)
            endpoint.close();
        endpoint = null;
    }

    private @Nonnull Stream<Term> transitiveClosure(boolean strict, @Nonnull Term start,
                                                    @Nonnull Property property) {
        Preconditions.checkArgument(start instanceof Res, "Term start must be a instance of Res");
        if (model == null) {
            warnEmpty();
            return Stream.empty();
        }
        Resource resource;
        if (start instanceof JenaTerm)
            resource = ((JenaTerm) start).getModelNode().asResource();
        else if (start instanceof Blank)
            return Stream.empty();
        else
            resource = model.createResource(start.asURI().getURI());
        return ModelUtils.closure(model, resource, property, true, strict)
                .map(JenaWrappers::fromJena);
    }

    @Override
    public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        return transitiveClosure(true, term, RDFS.subClassOf);
    }
    @Override public @Nonnull Stream<Term> withSubClasses(@Nonnull Term term) {
        return transitiveClosure(false, term, RDFS.subClassOf);
    }

    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        if (subClass.equals(superClass))
            return true;
        if (model == null) {
            warnEmpty();
            return false;
        }
        if (!subClass.isRes()) {
            logger.debug("Suspicious isSubClass({}, {}) call: {} is not a resource",
                         subClass, superClass, subClass);
            return false;
        }
        if (!superClass.isRes()) {
            logger.debug("Suspicious isSubClass({}, {}) call: {} is not a resource",
                         subClass, superClass, superClass);
            return false;
        }
        Node subRes = JenaWrappers.toJena(subClass).asResource().asNode();
        Node superRes = JenaWrappers.toJena(superClass).asResource().asNode();
        return hasPath(model.getGraph(), subRes, RDFS.subClassOf.asNode(), superRes);
    }

    private boolean hasPath(@Nonnull Graph graph, @Nonnull Node start, @Nonnull Node edge,
                            @Nonnull Node end) {
        Set<Node> visited = new HashSet<>();
        ArrayDeque<Node> stack = new ArrayDeque<>();
        stack.push(start);
        while (!stack.isEmpty()) {
            Node next = stack.pop();
            if (next.equals(end))
                return true;
            if (!visited.add(next))
                continue;
            for (ExtendedIterator<Triple> it = graph.find(next, edge, null); it.hasNext(); )
                stack.push(it.next().getObject());
        }
        return false;
    }
    @Override public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        return transitiveClosure(true, term, RDFS.subPropertyOf);
    }

    @Override public @Nonnull Stream<Term> withSubProperties(@Nonnull Term term) {
        return transitiveClosure(false, term, RDFS.subPropertyOf);
    }

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        if (subProperty.equals(superProperty))
            return true;
        if (model == null) {
            warnEmpty();
            return false;
        }
        if (!subProperty.isURI()) {
            logger.debug("Suspicious isSubProperty({}, {}): {} is not an IRI",
                         subProperty, superProperty, subProperty);
            return false;
        }
        if (!superProperty.isURI()) {
            logger.debug("Suspicious isSubProperty({}, {}): {} is not an IRI",
                         subProperty, superProperty, superProperty);
            return false;
        }
        Node start = JenaWrappers.toJena(subProperty).asNode();
        Node end = JenaWrappers.toJena(superProperty).asNode();
        return hasPath(model.getGraph(), start, RDFS.subPropertyOf.asNode(), end);
    }

    @Override
    public void close() {
        model = null;
    }

    @Override public @Nonnull String toString() {
        StringBuilder b = new StringBuilder("TransitiveClosureTBox");
        if (name != null)
            b.append('(').append(name).append(')');
        else
            b.append(String.format("@%x", System.identityHashCode(this)));
        return b.toString();
    }
}
