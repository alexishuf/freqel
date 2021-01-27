package br.ufsc.lapesd.freqel.testgen;

import com.google.common.base.Stopwatch;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.jena.riot.system.StreamRDFWriter.getWriterStream;

@SuppressWarnings("FieldMayBeFinal")
public class BSBMTestResourcesGenerator {
    private static final Logger logger = LoggerFactory.getLogger(BSBMTestResourcesGenerator.class);

    public static final String BSBM_NS = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";
    private static final String TYPE_URI = RDF.type.asNode().getURI();

    @Option(name = "--help", aliases = {"-h"}, help = true, usage = "Shows this help message")
    private boolean help;

    @Option(name = "--max-subj-window", usage = "Maximum distance between a triple ?x ?p ?o and " +
            "the triple ?x a ?class counted in number of distinct triple subjects.")
    private int maxSubjWindow = 50000;

    @Option(name = "--out-dir", required = true,
            usage = "Output directory where to write a TTL file for each class/data source")
    private File outputDir;

    @Argument
    private File datasetFile;

    public enum BSBMClass {
        Product,
        ProductType,
        ProductFeature,
        Producer,
        Vendor,
        Offer,
        Person,
        Review;

        private static final String[] URIS;
        private static final Resource[] RESOURCES;

        static {
            int length = BSBMClass.values().length;
            URIS = new String[length];
            RESOURCES = new Resource[length];
            for (int i = 0; i < length; i++) {
                BSBMClass value = BSBMClass.values()[i];
                if (value.equals(Person))
                    URIS[i] = FOAF.Person.getURI();
                else
                    URIS[i] = BSBM_NS + value.name();
                RESOURCES[i] = ResourceFactory.createResource(URIS[i]);
            }
        }

        public @Nonnull Resource asResource() {
            return RESOURCES[ordinal()];
        }
        public @Nonnull String asURIString() {
            return URIS[ordinal()];
        }

        public static @Nullable BSBMClass fromURI(@Nonnull String uri) {
            for (int i = 0; i < URIS.length; i++) {
                if (URIS[i].equals(uri))
                    return values()[i];
            }
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        BSBMTestResourcesGenerator app = new BSBMTestResourcesGenerator();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
            if (app.help)
                printHelp(parser, System.out);
            else
                app.run();
        } catch (CmdLineException e) {
            printHelp(parser, System.err);
            System.exit(1);
        }
    }

    private static void printHelp(@Nonnull CmdLineParser parser, @Nonnull PrintStream out) {
        out.printf("Usage: java -cp $JAR_PATH %s ", BSBMTestResourcesGenerator.class.getName());
        parser.printSingleLineUsage(out);
        out.println();
        parser.printUsage(out);
    }

    public void run() throws IOException {
        if (outputDir.exists() && !outputDir.isDirectory())
            throw new IOException(outputDir+" exists and is not a directory");
        if (!outputDir.exists() && !outputDir.mkdirs())
            throw new IOException("Could not mkdir "+outputDir);
        logger.info("Writing one TTL file per BSBM class to {}", outputDir);

        Stopwatch sw = Stopwatch.createStarted();
        try (FileInputStream inputStream = new FileInputStream(datasetFile);
             Scatter scatter = new Scatter()) {
            Lang lang = RDFLanguages.filenameToLang(datasetFile.getName(), Lang.TTL);
            RDFDataMgr.parse(scatter, inputStream, datasetFile.toURI().toString(), lang);
            int orphans = scatter.getOrphanTriples();
            if (orphans > 0)
                logger.error("There were {} orphan triples! This should not occur.", orphans);
            logger.info("Scattered {} triples in {}s", scatter.getTriplesWritten(),
                        sw.elapsed(TimeUnit.MILLISECONDS)/1000.0);
        }
    }

    private class Scatter extends StreamRDFBase implements Closeable {
        private final Map<BSBMClass, StreamRDF> cls2rdfStream;
        private final Map<BSBMClass, OutputStream> cls2outStream;
        private final LinkedHashMap<Node, List<Triple>> orphans;
        private final LinkedHashMap<Node, BSBMClass> subj2cls;
        private int triplesWritten;
        private boolean finished;

        public Scatter() throws FileNotFoundException {
            cls2rdfStream = new HashMap<>();
            cls2outStream = new HashMap<>();
            orphans = new LinkedHashMap<>();
            subj2cls = new LinkedHashMap<>();
            try {
                for (BSBMClass cls : BSBMClass.values()) {
                    File file = new File(outputDir, cls.name() + ".ttl");
                    OutputStream outStream = new FileOutputStream(file);
                    cls2outStream.put(cls, outStream);
                    cls2rdfStream.put(cls, getWriterStream(outStream, RDFFormat.TURTLE_BLOCKS));
                }
            } catch (Throwable t) {
                try {
                    close();
                } catch (IOException e) {
                    t.addSuppressed(e);
                }
                throw t;
            }
        }

        public int getTriplesWritten() {
            return triplesWritten;
        }

        public int getOrphanTriples() {
            return orphans.values().stream().map(List::size).reduce(Integer::sum).orElse(0);
        }

        @Override
        public void triple(Triple triple) {
            Node subj = triple.getSubject();
            if (triple.getPredicate().getURI().equals(TYPE_URI)) {
                BSBMClass cls = BSBMClass.fromURI(triple.getObject().getURI());
                if (cls != null) {
                    BSBMClass old = subj2cls.put(subj, cls);
                    assert old == null;
                    if (subj2cls.size() > maxSubjWindow)
                        subj2cls.remove(subj2cls.keySet().iterator().next()); //remove oldest
                    List<Triple> triples = orphans.remove(subj);
                    if (triples != null)
                        triples.forEach(this::triple);
                    cls2rdfStream.get(cls).triple(triple);
                    ++triplesWritten;
                }
            } else {
                BSBMClass cls = subj2cls.get(subj);
                if (cls == null) {
                    List<Triple> list = orphans.computeIfAbsent(subj, k -> new ArrayList<>());
                    list.add(triple);
                    if (orphans.size() > maxSubjWindow)
                        orphans.remove(orphans.keySet().iterator().next()); //remove oldest
                } else {
                    cls2rdfStream.get(cls).triple(triple);
                    ++triplesWritten;
                }
            }
        }

        @Override
        public void quad(Quad quad) {
            triple(quad.asTriple());
        }

        @Override
        public void base(String base) {
            for (StreamRDF streamRDF : cls2rdfStream.values())
                streamRDF.base(base);
        }

        @Override
        public void prefix(String prefix, String iri) {
            for (StreamRDF streamRDF : cls2rdfStream.values())
                streamRDF.prefix(prefix, iri);
        }

        @Override
        public void finish() {
            finished = true;
            for (StreamRDF streamRDF : cls2rdfStream.values())
                streamRDF.finish();
        }

        @Override
        public void close() throws IOException {
            if (!finished) finish();

            IOException exception = null;
            for (Map.Entry<BSBMClass, OutputStream> e : cls2outStream.entrySet()) {
                try {
                    e.getValue().close();
                } catch (IOException ex) {
                    logger.error("Failed to close OutputStream for class {}", e.getKey(), ex);
                    if (exception == null) exception = ex;
                    else exception.addSuppressed(ex);
                }
            }
            if (exception != null)
                throw exception;
        }
    }
}
