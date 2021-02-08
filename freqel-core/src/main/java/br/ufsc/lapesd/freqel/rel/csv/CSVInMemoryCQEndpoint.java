package br.ufsc.lapesd.freqel.rel.csv;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.TrapDescription;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.jena.model.term.node.JenaNodeTermFactory;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.model.NTParseException;
import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdTermFactory;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.NoMergePolicyAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.decorators.EndpointDecorators;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.*;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.rel.common.AnnotationStatus;
import br.ufsc.lapesd.freqel.rel.common.RelationalMoleculeMatcher;
import br.ufsc.lapesd.freqel.rel.common.StarSubQuery;
import br.ufsc.lapesd.freqel.rel.common.StarsHelper;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.XSD;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.federation.SingletonSourceFederation.createFederation;
import static br.ufsc.lapesd.freqel.federation.SingletonSourceFederation.getInjector;
import static br.ufsc.lapesd.freqel.jena.JenaWrappers.toJenaNode;
import static br.ufsc.lapesd.freqel.rel.mappings.RelationalMappingUtils.predicate2column;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

/**
 * This is a simple non-indexed endpoint that queries a single in-memory table.
 *
 * This is mostly for example and testing purposes. For small CSVs, it may be better to
 * convert them into RDF a priori. For larger CSVs, maybe loading them into HSQLDB or other
 * embedded on-disk store would be better. them into hsqldb and query it instead.
 */
public class CSVInMemoryCQEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(CSVInMemoryCQEndpoint.class);
    private static final URI xsdString = new StdURI(XSD.xstring.getURI());

    private @Nonnull final IndexSet<String> columns;
    private @Nonnull final List<Column> columnObjects;
    private final int rows;
    private @Nonnull final List<Object> data;
    private @Nonnull final RelationalMapping mapping;
    private @Nonnull final Molecule molecule;
    private @Nonnull final MoleculeMatcher moleculeMatcher;
    private @Nullable Federation federation;
    private @Nonnull final SPARQLFilterExecutor filterExecutor = new JenaSPARQLFilterExecutor();

    /* --- --- --- Constructor & loader/parser --- --- --- */

    public CSVInMemoryCQEndpoint(@Nonnull Collection<String> columns, @Nonnull List<Object> data,
                                 @Nonnull RelationalMapping mapping) {
        super(TrapDescription.FACTORY);
        this.columns = FullIndexSet.from(columns);
        this.data = data;
        this.rows = data.size() / columns.size();
        assert rows * columns.size() == data.size();
        this.mapping = mapping;

        //Get table name and initialize columns
        Molecule tmpMol = mapping.createMolecule();
        String table = tmpMol.getCore().getTags().stream().filter(TableTag.class::isInstance)
                .map(t -> ((TableTag) t).getTable())
                .findFirst().orElse(null);
        Preconditions.checkArgument(table != null, "Missing TableTag on Molecule core");
        columnObjects = new ArrayList<>(columns.size());
        for (String column : columns)
            columnObjects.add(new Column(table, column));
        assert new ArrayList<>(columns).equals(columnObjects.stream().map(Column::getColumn)
                                                                     .collect(toList()));

        this.molecule = mapping.createMolecule(columnObjects);
        NoMergePolicyAnnotation policy = new NoMergePolicyAnnotation();
        TBox empty = new EmptyTBox();
        this.moleculeMatcher = new RelationalMoleculeMatcher(this.molecule, empty, policy);
    }

    /**
     * Tries to parse a string as a Turtle short form (e.g., 23, false, 1.2, etc.). If parsing
     * fails, returns the string as a string.
     *
     * Note: Alternative implementations may return any Object in addition to {@link String}s.
     * If the Object is not a {@link Term} instance, then it will be converted into one when
     * the results of a query.
     *
     * @param string what is to be parsed
     * @return A {@link Term} or the string itself
     */
    public static @Nullable Object defaultParser(@Nullable String string) {
        try {
            return RDFUtils.fromTurtleShortForm(string, JenaNodeTermFactory.INSTANCE);
        } catch (NTParseException e) {
            return JenaWrappers.fromJena(ResourceFactory.createTypedLiteral(string));
        }
    }

    public static class Loader {
        private final @Nonnull RelationalMapping mapping;
        private CSVFormat format = null;
        private List<String> columns = null;
        private final Map<String, Function<String, ?>> parsers = new HashMap<>();

        public Loader(@Nonnull RelationalMapping mapping) {
            this.mapping = mapping;
        }

        /**
         * Sets the columns of the file or a projection of columns.
         *
         * If {@link Loader#format(CSVFormat)} is omitted or has
         * {@link CSVFormat#withFirstRecordAsHeader()}, then this defines a projection of
         * the columns. Else this defines the columns and the first record of the input data is
         * considered to be data, not columns names.
         */
        public @Nonnull Loader withColumns(@Nonnull List<String> columns) {
            this.columns = columns;
            return this;
        }

        /**
         * Set the CSV format.
         *
         * The default is {@link CSVFormat#DEFAULT} with {@link CSVFormat#withFirstRecordAsHeader()}
         * and with '\t' as separator if {@link Loader#load(File)} is used with a .tsv.
         *
         * For the effect of {@link CSVFormat#withFirstRecordAsHeader()},
         * see {@link Loader#withColumns(List)}.
         */
        public @Nonnull Loader format(@Nonnull CSVFormat format) {
            this.format = format;
            return this;
        }

        public @Nonnull Loader parser(@Nonnull String column,
                                      @Nonnull Function<String, ?> parser) {
            parsers.put(column, parser);
            return this;
        }

        public @Nonnull CSVInMemoryCQEndpoint load(@Nonnull File file) throws IOException {
            if (format == null) {
                format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
                if (file.getName().toLowerCase().endsWith(".tsv"))
                    format = format.withDelimiter('\t');
            }
            try (FileInputStream inputStream = new FileInputStream(file)) {
                return load(inputStream);
            }
        }
        public @Nonnull CSVInMemoryCQEndpoint load(@Nonnull InputStream inputStream)
                throws IOException {
            try (Reader reader = new InputStreamReader(inputStream)) {
                return load(reader);
            }
        }
        public @Nonnull CSVInMemoryCQEndpoint load(@Nonnull InputStream inputStream,
                                                   @Nonnull Charset charset) throws IOException {
            try (Reader reader = new InputStreamReader(inputStream, charset)) {
                return load(reader);
            }
        }
        public @Nonnull CSVInMemoryCQEndpoint load(@Nonnull Reader reader) throws IOException {
            if (format == null)
                format = CSVFormat.DEFAULT.withFirstRecordAsHeader();
            try (CSVParser parser = new CSVParser(reader, format)) {
                List<String> actualColumns = columns == null ? parser.getHeaderNames() : columns;
                Set<String> missing;
                if (columns != null && format.getSkipHeaderRecord()) {
                    missing = CollectionUtils.setMinus(columns, parser.getHeaderNames());
                    if (!missing.isEmpty())
                        logger.warn("Projection includes columns missing in CSV: {}", missing);
                } else {
                    missing = Collections.emptySet();
                }
                Function<String, ?> defP = CSVInMemoryCQEndpoint::defaultParser;
                ArrayList<Object> data = new ArrayList<>(1024);
                for (CSVRecord record : parser) {
                    for (String col : actualColumns) {
                        if (missing.contains(col))
                            data.add(null);
                        else
                            data.add(parsers.getOrDefault(col, defP).apply(record.get(col)));
                    }
                }
                data.trimToSize();
                return new CSVInMemoryCQEndpoint(actualColumns, data, mapping);
            }
        }
    }

    public static @Nonnull Loader loader(@Nonnull RelationalMapping mapping) {
        return new Loader(mapping);
    }

    /* --- --- --- (protected) data access methods --- --- -- */

    protected @Nullable Term get(int row, @Nonnull String col) {
        int idx = columns.indexOf(col);
        if (idx < 0) {
            logger.debug("Request for bad column {} at row {} will return null.", col, row);
            return null;
        }
        return get(row, idx);
    }
    protected @Nullable Term get(int row, int col) {
        int colCount = this.columns.size();
        Preconditions.checkElementIndex(row, rows);
        Preconditions.checkElementIndex(col, colCount);
        int idx = row * colCount + col;
        assert idx < data.size();
        Object o = data.get(idx);
        if (o == null) return null;
        if (o instanceof Term) return (Term) o;
        if (o instanceof Node) return JenaWrappers.fromJena((Node)o);
        if (o instanceof RDFNode) return JenaWrappers.fromJena((RDFNode) o);
        try {
            return RDFUtils.fromTurtleShortForm(Objects.toString(o), StdTermFactory.INSTANCE);
        } catch (NTParseException ignored) { }
        return StdLit.fromUnescaped(o.toString(), xsdString);
    }
    protected @Nonnull List<Object> getRowValues(int row) {
        Preconditions.checkElementIndex(row, rows);
        int colCount = this.columns.size();
        int base = row * colCount;
        return data.subList(base, base + colCount);
    }

    /* --- --- --- convenience methods for matching & description --- --- -- */

    public @Nonnull RelationalMapping getMapping() {
        return mapping;
    }
    public @Nonnull Molecule getMolecule() {
        return molecule;
    }
    @Override public @Nonnull MoleculeMatcher getDescription() {
        return moleculeMatcher;
    }

    /* --- --- --- Interface implementation --- --- --- */

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        AnnotationStatus st = new AnnotationStatus(query);
        if (!st.isValid()) {
            if (st.isEmpty()) return getFederation().query(query);
            else              st.checkNotPartiallyAnnotated(); //throws IllegalArgumentException
        }
        List<StarSubQuery> stars = StarsHelper.findStars(query);
        if (stars.size() == 1) {
            //noinspection AssertWithSideEffects
            assert query.attr().isJoinConnected();
            assert stars.get(0).getFilters().containsAll(query.getModifiers().filters());
            boolean distinct = query.getModifiers().distinct() != null;
            Results results = queryStar(stars.get(0), distinct);
            assert !distinct || results.isDistinct();
            results = ProjectingResults.applyIf(results, query);
            results = LimitResults.applyIf(results, query);
            return AskResults.applyIf(results, query);
        } else {
            List<Op> leaves = new ArrayList<>();
            for (StarSubQuery star : stars) {
                MutableCQuery q = MutableCQuery.from(star.getTriples());
                q.mutateModifiers().addAll(star.getFilters());
                leaves.add(new EndpointQueryOp(this, q));
            }
            ConjunctivePlanner planner = getInjector().getInstance(ConjunctivePlanner.class);
            return getFederation().execute(planner.plan(query, leaves));
        }
    }

    private @Nonnull Federation getFederation() {
        if (federation == null)
            federation = createFederation(EndpointDecorators.uncloseable(this));
        return federation;
    }

    private @Nonnull CollectionResults queryStar(@Nonnull StarSubQuery star, boolean distinct) {
        if (star.getTriples().isEmpty()) {
            assert false;
            return CollectionResults.empty(emptySet());
        }
        Term core = star.getCore();
        Node coreNode = toJenaNode(core);
        List<Selector> selectors = new ArrayList<>();
        star.getTriples().forEach(t -> selectors.add(new Selector(t)));
        SolutionBuilder sb = new SolutionBuilder(star);
        Collection<Solution> solutions = distinct ? new HashSet<>() : new ArrayList<>();

        rows_loop:
        for (int i = 0; i < rows; i++) {
            sb.reset();
            for (Selector selector : selectors) {
                Term term = selector.match(i);
                if (term == null)
                    continue rows_loop;
                String varName = selector.getVarName();
                if (varName != null && !sb.offer(term, varName))
                    continue rows_loop;
            }
            Term subj = mapping.getNameFor(columnObjects, getRowValues(i));
            if (core.isVar()) {
                if (!sb.offer(subj, core.asVar().getName())) continue;
            } else if (!JenaWrappers.toJenaNode(subj).matches(coreNode)) {
                continue;
            }
            Solution solution = sb.getSolution();
            if (solution != null) //may be null if filters reject
                solutions.add(solution);
        }
        return new CollectionResults(solutions, sb.vars);
    }

    private class SolutionBuilder {
        final IndexSet<String> vars;
        final IndexSubset<String> matched;
        final ArraySolution.ValueFactory factory;
        final List<Term> values;
        final StarSubQuery star;

        public SolutionBuilder(@Nonnull StarSubQuery star) {
            this.star = star;
            List<String> varList = star.getTriples().stream().map(Triple::getObject)
                                        .filter(Term::isVar)
                                        .map(t -> t.asVar().getName()).collect(toList());
            Term core = star.getCore();
            if (core.isVar())
                varList.add(core.asVar().getName());
            vars = FullIndexSet.from(varList);

            matched = vars.emptySubset();
            factory = ArraySolution.forVars(vars);
            int size = vars.size();
            values = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                values.add(null);
        }

        public void reset() {
            int size = values.size();
            for (int i = 0; i < size; i++)
                values.set(i, null);
            assert values.size() == vars.size();
            matched.clear();
        }

        public boolean offer(@Nullable Term term, @Nonnull String var) {
            int idx = vars.indexOf(var);
            assert idx >= 0;
            assert matched.getParent() == vars;
            assert values.size() == vars.size();
            if (matched.getBitset().get(idx)) {
                Node offer = toJenaNode(term), old = toJenaNode(values.get(idx));
                return old.matches(offer);
            } else {
                values.set(idx, term);
                matched.getBitset().set(idx);
                return true;
            }
        }

        public @Nullable Solution getSolution() {
            ArraySolution s = factory.fromValues(values);
            boolean ok = star.getFilters().stream().allMatch(f ->  filterExecutor.evaluate(f, s));
            return ok ? s : null;
        }
    }

    private class Selector {
        final @Nonnull Triple triple;
        final @Nonnull List<Integer> colIndices;
        private final @Nonnull Node oNode;

        Selector(@Nonnull Triple triple) {
            this.colIndices = new ArrayList<>();
            for (Column c : predicate2column(molecule, triple.getPredicate())) {
                int idx = CSVInMemoryCQEndpoint.this.columns.indexOf(c.column);
                if (idx >= 0) {
                    colIndices.add(idx);
                }
            }
            this.triple = triple;
            this.oNode = toJenaNode(triple.getObject());
        }

        public @Nullable String getVarName() {
            return oNode.isVariable() ? oNode.getName() : null;
        }

        @Nullable Term match(int row) {
            for (int idx : colIndices) {
                Term actual = get(row, idx);
                if (actual != null && (oNode.isVariable() || toJenaNode(actual).matches(oNode)))
                    return actual;
            }
            return null;
        }
    }

    @Override public double alternativePenalty(@NotNull CQuery query) {
        return 0.76;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability) {
            case ASK:
            case CARTESIAN:
            case SPARQL_FILTER:
                return true;
            default:
                return hasRemoteCapability(capability);
        }
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case LIMIT:
                return true;
            default: // ASK & SPARQL_FILTER are not efficient, others are not supported
                return false;
        }
    }

    @Override
    public void close() {
        if (federation != null) {
            this.federation.close();
            federation = null;
        }
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public String toString() {
        return String.format("CSVInMemoryCQEndpoint{%s,cols=%s}",
                             molecule.getCore(), columns);
    }
}
