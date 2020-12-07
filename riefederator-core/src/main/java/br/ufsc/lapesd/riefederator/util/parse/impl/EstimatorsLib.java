package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.hdt.util.HDTUtils;
import br.ufsc.lapesd.riefederator.util.parse.RDFInputStream;
import br.ufsc.lapesd.riefederator.util.parse.RDFIterationDispatcher;
import br.ufsc.lapesd.riefederator.util.parse.RDFSyntax;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EstimatorsLib {
    public static void registerAll(@Nonnull RDFIterationDispatcher dispatcher) {
        LibHelper.registerAll(EstimatorsLib.class, Function.class, dispatcher);
    }

    @TargetClass(File.class)
    public static class FileEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            File file = (File) o;
            try {
                RDFInputStream ris = new RDFInputStream((File) o);
                RDFSyntax syntax = ris.getSyntaxOrGuess();
                if (syntax == RDFSyntax.HDT) {
                    HDTUtils.NullProgressListener listener = new HDTUtils.NullProgressListener();
                    try (HDT hdt = HDTManager.mapHDT(file.getAbsolutePath(), listener)) {
                        return hdt.size();
                    }
                } else {
                    long count = 0;
                    InputStream is = ris.getInputStream();
                    for (int c = is.read(); c > -1; c = is.read()) {
                        if (c == '\n') ++count;
                    }
                    return count;
                }
            } catch (IOException ignored) { }

            long count = 0;
            try (Reader reader = new InputStreamReader(new FileInputStream((File)o), UTF_8)) {
                for (int c = reader.read(); c > -1; c = reader.read()) {
                    if (c == '\n') ++count;
                }
            } catch (IOException e) {
                return -1L;
            }
            return count;
        }
    }

    @TargetClass(URI.class)
    public static class URIEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            URI u = (URI) o;
            if (u.getScheme().startsWith("file"))
                return dispatcher.estimate(Paths.get(u).toFile());
            return -1L;
        }
    }

    @TargetClass(String.class)
    public static class StringEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            String string = o.toString();
            if (string.startsWith("file:")) {
                try {
                    return dispatcher.estimate(Paths.get(new URI(string)).toFile());
                } catch (URISyntaxException ignored) { }
            } else if (!string.matches("^\\w+:.*")) {
                return dispatcher.estimate(new File(string));
            }
            return -1L;
        }
    }

    @TargetClass(Graph.class)
    public static class GraphEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            return Long.valueOf(((Graph)o).size());
        }
    }

    @TargetClass(Model.class)
    public static class ModelEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            return ((Model)o).size();
        }
    }

    @TargetClass(HDT.class)
    public static class HDTEstimator extends TripleCountEstimatorBase {
        @Override public long estimate(@Nonnull Object o) {
            return ((HDT)o).size();
        }
    }
}
