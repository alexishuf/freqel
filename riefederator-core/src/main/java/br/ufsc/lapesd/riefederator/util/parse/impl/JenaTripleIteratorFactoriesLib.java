package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.util.HDTUtils;
import br.ufsc.lapesd.riefederator.util.parse.*;
import br.ufsc.lapesd.riefederator.util.parse.iterators.ClosingHDTJenaTripleIterator;
import br.ufsc.lapesd.riefederator.util.parse.iterators.DelegatingJenaTripleIterator;
import br.ufsc.lapesd.riefederator.util.parse.iterators.HDTJenaTripleIterator;
import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

public class JenaTripleIteratorFactoriesLib {
    public static void registerAll(@Nonnull RDFIterationDispatcher dispatcher) {
        LibHelper.registerAll(JenaTripleIteratorFactoriesLib.class, JenaTripleIteratorFactory.class, dispatcher);
    }

    @TargetClass(Graph.class)
    public static class GraphFactory extends JenaTripleIteratorFactoryBase {
        @Override
        public @Nonnull JenaTripleIterator create(@Nonnull Object source) throws SourceIterationException {
            return new DelegatingJenaTripleIterator(((Graph) source).find());
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            return source instanceof Graph;
        }
    }

    @TargetClass(Model.class)
    public static class ModelFactory extends JenaTripleIteratorFactoryBase {
        @Override
        public @Nonnull JenaTripleIterator create(@Nonnull Object source) throws SourceIterationException {
            return new DelegatingJenaTripleIterator(((Model)source).getGraph().find());
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            return source instanceof Model;
        }
    }

    @TargetClass(HDT.class)
    public static class HDTFactory extends JenaTripleIteratorFactoryBase {
        private static final Logger logger = LoggerFactory.getLogger(HDTFactory.class);

        @Override public @Nonnull JenaTripleIterator
        create(@Nonnull Object source) throws SourceIterationException {
            HDT hdt = (HDT) source;
            try {
                IteratorTripleString it = hdt.search(null, null, null);
                return new HDTJenaTripleIterator(source, it);
            } catch (NotFoundException e) {
                throw new SourceIterationException(source, e);
            }
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            return source instanceof HDT;
        }
    }

    @TargetClass(File.class)
    public static class HDTFileFactory extends JenaTripleIteratorFactoryBase {
        @Override
        public @Nonnull JenaTripleIterator
        create(@Nonnull Object source) throws SourceIterationException {
            String path = ((File) source).getAbsolutePath();
            try {
                HDT hdt = HDTManager.mapHDT(path, new HDTUtils.NullProgressListener());
                IteratorTripleString it = hdt.search(null, null, null);
                return new ClosingHDTJenaTripleIterator(source, it, hdt);
            } catch (IOException e) {
                throw new SourceIterationException(source, "IOException reading "+path, e);
            } catch (NotFoundException e) {
                throw new SourceIterationException(source, "HDT file likely corrupted: "+path, e);
            }
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            if (!(source instanceof File))
                return false;
            try (RDFInputStream ris = new RDFInputStream((File) source)) {
                return ris.getSyntaxOrGuess() == RDFSyntax.HDT;
            } catch (IOException ignored) { return true; }
        }
    }

    @TargetClass(RDFInputStream.class)
    public static class HDTRDFInputStreamFactory extends JenaTripleIteratorFactoryBase {
        @Override public @Nonnull JenaTripleIterator
        create(@Nonnull Object src) throws SourceIterationException {
            HDT hdt;
            try (RDFInputStream ris = (RDFInputStream) src) {
                hdt = HDTManager.loadHDT(ris.getInputStream(), HDTUtils.NULL_LISTENER);
            } catch (IOException e) {
                throw new SourceIterationException(src, "Failed to load HDT from "+src, e);
            }
            try {
                IteratorTripleString it = hdt.search(null, null, null);
                return new ClosingHDTJenaTripleIterator(src, it, hdt);
            } catch (NotFoundException e) {
                throw new SourceIterationException(src, "HDT in stream likely corrupted:"+src, e);
            }
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            if (!(source instanceof RDFInputStream)) return false;
            return ((RDFInputStream)source).getSyntaxOrGuess() == RDFSyntax.HDT;
        }
    }

    @TargetClass(TBoxSpec.class)
    public static class TBoxSpecFactory extends JenaTripleIteratorFactoryBase {
        private static final Node imports = OWL2.imports.asNode();

        @Override public @Nonnull JenaTripleIterator
        create(@Nonnull Object source) throws SourceIterationException {
            TBoxSpec spec = (TBoxSpec) source;
            boolean fetchImports = spec.getFetchOwlImports();
            TBoxLoader loader = new TBoxLoader().fetchingImports(fetchImports);

            return new DelegatingJenaTripleIterator(dispatcher.parseAll(spec.getAllSources())) {
                ExtendedIterator<Triple> fetchedIt = null;

                @Override public boolean hasNext() {
                    boolean hasNext = super.hasNext();
                    if (!hasNext && fetchImports) {
                        if (fetchedIt == null)
                            fetchedIt = loader.getModel().getGraph().find();
                        hasNext = fetchedIt.hasNext();
                    }
                    return hasNext;
                }

                @Override public @Nonnull Triple next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    if (fetchedIt == null) {
                        Triple next = super.next();
                        if (fetchImports && next.getPredicate().equals(imports)) {
                            Node o = next.getObject();
                            if (o.isURI())
                                loader.fetchOntology(o.getURI());
                        }
                        return next;
                    } else {
                        return fetchedIt.next();
                    }
                }
            };
        }

        @Override public boolean canCreate(@Nonnull Object source) {
            return source instanceof TBoxSpec;
        }
    }
}
