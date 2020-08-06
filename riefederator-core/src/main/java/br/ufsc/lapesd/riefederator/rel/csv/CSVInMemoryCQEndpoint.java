package br.ufsc.lapesd.riefederator.rel.csv;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.model.term.node.JenaNodeTermFactory;
import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.*;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJenaNode;
import static br.ufsc.lapesd.riefederator.rel.mappings.RelationalMappingUtils.predicate2column;
import static java.util.Collections.emptyList;
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

    private @Nonnull final IndexedSet<String> columns;
    private @Nonnull final List<Column> columnObjects;
    private final int rows;
    private @Nonnull final List<Object> data;
    private @Nonnull final RelationalMapping mapping;
    private @Nonnull final Molecule molecule;
    private @Nonnull final MoleculeMatcher moleculeMatcher;

    /* --- --- --- Constructor & loader/parser --- --- --- */

    public CSVInMemoryCQEndpoint(@Nonnull Collection<String> columns, @Nonnull List<Object> data,
                                 @Nonnull RelationalMapping mapping) {
        this.columns = IndexedSet.from(columns);
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
        this.moleculeMatcher = new MoleculeMatcher(molecule, new TransitiveClosureTBoxReasoner());
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
        private Map<String, Function<String, ?>> parsers = new HashMap<>();

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
    public @Nonnull MoleculeMatcher getDefaultMatcher() {
        return moleculeMatcher;
    }
    public @Nonnull Source asSource() {
        return new Source(getDefaultMatcher(), this);
    }

    /* --- --- --- Interface implementation --- --- --- */

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        if (query.isEmpty())
            return CollectionResults.empty(emptyList());
        Results results;
        if (!query.attr().isJoinConnected()) { // code after this if cannot deal with cartesians
            results = StarsHelper.executeJoinDisconnected(query, this);
        } else {
            results = queryConnected(query);
            results = SPARQLFilterResults.applyIf(results, query);
            results = ProjectingResults.applyIf(results, query);
        }
        results = LimitResults.applyIf(results, query);
        return results;
    }

    /**
     * Process this as a join between stars (join-connectivity among stars assumed)
     */
    private @Nonnull Results queryConnected(@Nonnull CQuery query) {
        boolean distinct = ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null;
        IndexedSet<SPARQLFilter> allFilters = null;
        IndexedSubset<SPARQLFilter> pendingFilters = null;
        CollectionResults working = null;
        for (StarSubQuery star : StarsHelper.findStars(query)) {
            if (allFilters == null) {
                allFilters = star.getAllQueryFilters();
                pendingFilters = allFilters.fullSubset();
            } else {
                assert allFilters.equals(star.getAllQueryFilters());
                pendingFilters.removeAll(star.getFilters());
            }
            CollectionResults right = queryStar(star, distinct);
            working = working != null ? hashJoin(working, right) : right;
        }
        assert working != null;
        return SPARQLFilterResults.applyIf(working, pendingFilters);
    }

    private @Nonnull CollectionResults hashJoin(@Nonnull CollectionResults a,
                                                @Nonnull CollectionResults b) {
        if (b.getCollection().size() < a.getCollection().size()) {
            CollectionResults tmp = b;
            b = a;
            a = tmp;
        }
        Set<String> av = a.getVarNames();
        Set<String> bv = b.getVarNames();
        Set<String> all = CollectionUtils.union(av, bv);
        Set<String> jv = CollectionUtils.intersect(av, bv);
        assert !jv.isEmpty();
        return CollectionResults.greedy(new InMemoryHashJoinResults(a, b, jv, all, false));
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
            Solution solution = sb.takeSolution();
            if (solution != null) //may be null if filters reject
                solutions.add(solution);
        }
        return new CollectionResults(solutions, sb.vars);
    }

    private static class SolutionBuilder {
        final IndexedSet<String> vars;
        final IndexedSubset<String> matched;
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
            vars = IndexedSet.from(varList);

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
            if (matched.getBitSet().get(idx)) {
                Node offer = toJenaNode(term), old = toJenaNode(values.get(idx));
                return old.matches(offer);
            } else {
                values.set(idx, term);
                matched.getBitSet().set(idx);
                return true;
            }
        }

        public @Nullable Solution takeSolution() {
            ArraySolution solution = factory.fromValues(values);
            reset();
            boolean ok = star.getFilters().stream().allMatch(f -> f.evaluate(solution));
            return ok ? solution : null;
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

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability) {
            case ASK:
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

    /* --- --- --- Object methods --- --- --- */

    @Override
    public String toString() {
        return String.format("CSVInMemoryCQEndpoint{%s,cols=%s}",
                             molecule.getCore(), columns);
    }
}
