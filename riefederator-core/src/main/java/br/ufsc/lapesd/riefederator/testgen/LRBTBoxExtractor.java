package br.ufsc.lapesd.riefederator.testgen;

import com.google.common.collect.Sets;
import org.apache.jena.atlas.io.Writer2;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterTTL;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LRBTBoxExtractor {
    private static final Logger logger = LoggerFactory.getLogger(LRBTBoxExtractor.class);

    @Option(name = "--help", aliases = {"-h"}, help = true, usage = "Shows this help")
    private boolean help;

    @Option(name = "--out-dir", aliases = {"-o"},
            usage = "Destination root for test resources. Will be created if does not exist")
    private File outDir;

    @Option(name = "--fail-missing-dumps",
            usage = "Abort if a dump file is missing from --dumps-dir")
    private boolean failOnMissingDumps;

    @Option(name = "--dumps-dir", required = true, aliases = {"-d"},
            usage = "Directory with dumps (one compressed archive per dataset)")
    private File dumpsDir;

    @Option(name = "--fast", aliases = {"-f"},
            usage = "Use more disk space for faster processing")
    private boolean fast = false;

    private static final Set<Node> tBoxPredicates = Sets.newHashSet(
            RDFS.domain.asNode(),
            RDFS.range.asNode(),
            RDFS.subPropertyOf.asNode(),
            RDFS.subClassOf.asNode(),
            OWL2.equivalentClass.asNode(),
            OWL2.equivalentProperty.asNode(),
            OWL2.unionOf.asNode(),
            OWL2.complementOf.asNode(),
            OWL2.oneOf.asNode(),
            OWL2.intersectionOf.asNode()
    );
    private static final Set<Node> classTypes = Sets.newHashSet(
            RDFS.Class.asNode(),
            OWL2.Class.asNode()
    );
    private static final Set<Node> propertyTypes = Sets.newHashSet(
            RDF.Property.asNode(),
            OWL2.ObjectProperty.asNode(),
            OWL2.DatatypeProperty.asNode()
    );
    private static final Node type = RDF.type.asNode();

    public static void main(String[] args) throws Exception {
        LRBTBoxExtractor app = new LRBTBoxExtractor();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
            if (app.help) showHelp(parser, System.out);
            else          app.run();
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            showHelp(parser, System.err);
        }
    }

    private static void showHelp(@Nonnull CmdLineParser parser, @Nonnull PrintStream out) {
        out.printf("java -cp $JAR_PATH %s ", LRBTBoxExtractor.class.getName());
        parser.printSingleLineUsage(out);
        out.println("\n\nOptions:");
        parser.printUsage(out);
    }

    private void run() throws Exception {
        if (outDir == null)
            outDir = dumpsDir;
        if (!dumpsDir.exists() || !dumpsDir.isDirectory())
            throw new IOException("Dumps directory does not exist: "+dumpsDir);
        if (!dumpsDir.isDirectory())
            throw new IOException("Given dumps directory "+dumpsDir+" is not a directory");
        if (!outDir.exists() && !outDir.mkdirs())
            throw new IOException("Could not create output dir "+outDir);
        else if (!outDir.isDirectory())
            throw new IOException("Given --out-dir "+outDir+" is not a directory");
        for (LRBDataset ds : LRBDataset.values()) {
            File dumpFile = ds.getDumpFile(dumpsDir);
            if (dumpFile == null && failOnMissingDumps)
                throw new IllegalArgumentException("Missing dump for "+ds);
            else if (!dumpFile.canRead())
                throw new IOException("Cannot read dump file "+dumpFile);
        }
        if (fast) {
            IOException[] ex = {null};
            Arrays.stream(LRBDataset.values()).parallel().forEach(ds -> {
                try {
                    extractTBoxFast(ds);
                } catch (IOException e) {
                    if (ex[0] == null) ex[0] = e;
                    else               ex[0].addSuppressed(e);
                }
            });
            if (ex[0] != null)
                throw ex[0];
        } else {
            for (LRBDataset ds : LRBDataset.values())
                extractTBoxSlow(ds);
        }
    }

    private class DatasetState implements Closeable {
        private final @Nonnull File dumpFile;
        private final @Nonnull File ontoFile;
        private final @Nonnull Writer onto, classes, predicates;
        private final @Nonnull Writer2 jenaOntoWriter;
        private @Nonnull Set<String> subjects, predicatesSet, classesSet;
        private final @Nonnull PrefixMap pm;

        public DatasetState(@Nonnull LRBDataset ds, boolean parallel) throws IOException {
            dumpFile = ds.getDumpFile(dumpsDir);
            ontoFile = new File(outDir, ds.baseName() + ".ttl");
            File classesFile = new File(outDir, ds.baseName() + ".classes");
            File propertiesFile = new File(outDir, ds.baseName() + ".properties");
            onto = new OutputStreamWriter(new FileOutputStream(ontoFile), UTF_8);
            jenaOntoWriter = Writer2.wrap(onto);
            classes = new OutputStreamWriter(new FileOutputStream(classesFile), UTF_8);
            predicates = new OutputStreamWriter(new FileOutputStream(propertiesFile), UTF_8);
            if (parallel) {
                subjects = Collections.synchronizedSet(new HashSet<>());
                predicatesSet = Collections.synchronizedSet(new HashSet<>());
                classesSet = Collections.synchronizedSet(new HashSet<>());
            } else {
                subjects = new HashSet<>();
                predicatesSet = new HashSet<>();
                classesSet = new HashSet<>();
            }
            pm = new PrefixMapStd();
        }

        public synchronized @Nullable String addPrefix(@Nonnull String prefix,
                                                       @Nonnull String iri) {
            String old = pm.expand(prefix, "");
            if (old == null) {
                pm.add(prefix, iri);
            } else if (!old.equals(iri) && pm.abbrev(iri) == null) {
                int suffix = 1;
                while (pm.contains(prefix+suffix)) ++suffix;
                prefix = prefix+suffix;
                pm.add(prefix, iri);
            } else {
                return null; // duplicate prefix
            }
            return prefix;
        }

        public void endFirstPass() throws IOException {
            ArrayList<String> predicatesList = new ArrayList<>(predicatesSet);
            ArrayList<String> classesList = new ArrayList<>(classesSet);
            predicatesSet = Collections.emptySet();
            classesSet = Collections.emptySet();
            Collections.sort(predicatesList);
            Collections.sort(classesList);
            for (String uri : predicatesList)
                predicates.write(uri+"\n");
            for (String uri : classesList)
                classes.write(uri+"\n");
            predicates.close();
            classes.close();
            logger.info("Write {} classes and {} predicates after first pass of {}",
                        predicatesList.size(), classesList.size(), dumpFile);
        }

        @Override public void close() throws IOException {
            logger.info("Completed 2 passes over {}", dumpFile);
            classes.close();
            predicates.close();
            jenaOntoWriter.close();
            onto.close();
        }
    }

    private void extractTBoxFast(@Nonnull LRBDataset ds) throws IOException {
        try (DatasetState st = new DatasetState(ds, true);
             SevenZipRDFStream.TempDir dir = new SevenZipRDFStream(st.dumpFile).extractAll()) {
            dir.parallelStreamAll(firstPass(st));
            st.endFirstPass();
            dir.parallelStreamAll(secondPass(st));
        }
    }

    private void extractTBoxSlow(@Nonnull LRBDataset ds) throws IOException {
        try (DatasetState st = new DatasetState(ds, false)) {
            new SevenZipRDFStream(st.dumpFile).streamAll(firstPass(st));
            st.endFirstPass();
            new SevenZipRDFStream(st.dumpFile).streamAll(secondPass(st));
        }
    }

    private @Nonnull StreamRDF firstPass(@Nonnull DatasetState st) {
        return new StreamRDFBase() {
            @Override public void triple(Triple t) {
                Node s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
                st.predicatesSet.add(p.getURI());
                boolean isType = p.equals(type);
                if (isType && o.isURI())
                    st.classesSet.add(o.getURI());
                if (s.isURI()) {
                    boolean ok = tBoxPredicates.contains(p);
                    if (!ok && isType)
                        ok = classTypes.contains(o) || propertyTypes.contains(o);
                    if (ok)
                        st.subjects.add(s.getURI());
                }
            }

            @Override public void prefix(String prefix, String iri) {
                prefix = st.addPrefix(prefix, iri);
                if (prefix == null)
                    return; //iri already has a prefix, do not output
                try {
                    synchronized (st.onto) {
                        st.onto.write("@prefix " + prefix + ": <" + iri + "> .\n");
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error writing to "+st.ontoFile, e);
                }
            }

            @Override public void base(String base) {
                prefix("base", base);
            }
        };
    }

    private @Nonnull StreamRDF secondPass(@Nonnull DatasetState st) {
        NodeFormatter formatter = new NodeFormatterTTL(null, st.pm);
        return new StreamRDFBase() {
            @Override public void triple(Triple triple) {
                Node s = triple.getSubject();
                if (s.isURI() && st.subjects.contains(s.getURI())) {
                    synchronized (st.jenaOntoWriter) {
                        formatter.format(st.jenaOntoWriter, s);
                        st.jenaOntoWriter.write(' ');
                        formatter.format(st.jenaOntoWriter, triple.getPredicate());
                        st.jenaOntoWriter.write(' ');
                        formatter.format(st.jenaOntoWriter, triple.getObject());
                        st.jenaOntoWriter.write(" .\n");
                    }
                }
            }
        };
    }
}
