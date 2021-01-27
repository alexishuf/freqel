package br.ufsc.lapesd.freqel.rel.mappings.r2rml;

import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermParser;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.RRFactory;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TriplesMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.RRException;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.RRMappingException;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl.AtomNameSelector;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl.TriplesMapContext;
import br.ufsc.lapesd.freqel.rel.sql.impl.NaturalSqlTermParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.vocabulary.OWL2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.InputStream;
import java.util.*;

import static br.ufsc.lapesd.freqel.rel.mappings.RelationalMappingUtils.getTable;

public class RRMapping implements RelationalMapping {
    private static final Logger logger = LoggerFactory.getLogger(RRMapping.class);

    static {
        RRFactory.install();
    }

    private @Nonnull final Multimap<String, TriplesMapContext> table2contexts;
    private @Nonnull final RelationalTermParser sqlTermParser;
    private final boolean strict;
    private final @Nullable String name;
    private final @Nonnull String baseURI;
    private @LazyInit Molecule molecule;

    public RRMapping(@Nonnull Model model, @Nonnull RelationalTermParser sqlTermParser,
                     @Nullable String name, boolean strict, @Nonnull String baseURI) {
        this.sqlTermParser = sqlTermParser;
        this.strict = strict;
        this.name = name;
        this.baseURI = baseURI;
        table2contexts = HashMultimap.create();
        StmtIterator it = model.listStatements(null, RR.logicalTable, (RDFNode) null);
        while (it.hasNext()) {
            TriplesMap node = it.next().getSubject().as(TriplesMap.class);
            TriplesMapContext ctx = new TriplesMapContext(node);
            table2contexts.put(ctx.getTable(), ctx);
        }
    }

    public static class Builder {
        private boolean strict = RRMapping.class.desiredAssertionStatus();
        private @Nonnull
        RelationalTermParser termParser = NaturalSqlTermParser.INSTANCE;
        private @Nullable String name = null;
        private @Nullable String baseURI = null;
        private @Nullable String mappingBaseURI = null;
        private @Nullable Lang lang = null;

        public @Nonnull Builder name(@Nonnull String name) {
            this.name = name;
            return this;
        }

        public @Nonnull Builder sqlTermParser(@Nonnull RelationalTermParser termParser) {
            this.termParser = termParser;
            return this;
        }

        public @Nonnull Builder strict(boolean value) {
            this.strict = value;
            return this;
        }

        public @Nonnull Builder mappingBaseURI(@Nonnull String value) {
            this.mappingBaseURI = value;
            return this;
        }

        public @Nonnull Builder baseURI(@Nonnull String value) {
            this.baseURI = value;
            return this;
        }

        public @Nonnull Builder lang(@Nonnull Lang lang) {
            this.lang = lang;
            return this;
        }

        public @Nonnull RRMapping load(@Nonnull Model model) {
            return new RRMapping(model, termParser, name, strict, baseURI == null ? "" : baseURI);
        }

        public @Nonnull RRMapping load(@Nonnull File file) {
            return loadFromURI(file.toURI().toString());
        }

        public @Nonnull RRMapping loadFromURI(@Nonnull String uri) {
            if (name == null)
                name = uri;
            Model model = ModelFactory.createDefaultModel();
            Lang lang = this.lang == null ? RDFLanguages.filenameToLang(uri) : this.lang;
            RDFDataMgr.read(model, uri, lang);
            return load(model);
        }

        public @Nonnull RRMapping load(@Nonnull InputStream inputStream) {
            Model model = ModelFactory.createDefaultModel();
            String readBase = mappingBaseURI == null ? baseURI : mappingBaseURI;
            if (this.lang != null) {
                RDFDataMgr.read(model, inputStream, readBase, lang);
            } else {
                RiotException exception = null;
                for (Lang lang : Arrays.asList(Lang.TTL, Lang.RDFXML, Lang.RDFJSON)) {
                    try {
                        RDFDataMgr.read(model, inputStream, readBase, lang);
                        exception = null;
                        break;
                    } catch (RiotException e) {
                        logger.info("No language hint and could not guess from path, " +
                                    "Trying {} failed", lang, e);
                        if (exception == null) exception = e;
                    }
                }
                if (exception != null)
                    throw exception;
            }
            return load(model);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    /* --- --- --- Internals --- --- --- */

    private @Nonnull Collection<String> asColumnNames(@Nonnull String table,
                                                      @Nullable Collection<?> columns) {
        Collection<String> columnNames;
        if (columns == null) {
            columnNames = Collections.emptySet();
        } else if (columns.stream().allMatch(String.class::isInstance)) {
            //noinspection unchecked
            columnNames = (Collection<String>) columns;
        } else {
            columnNames = new HashSet<>();
            for (Object o : columns) {
                if (o instanceof String) {
                    columnNames.add((String)o);
                } else {
                    Column col = (Column)o;
                    if (col.getTable().equals(table))
                        columnNames.add(col.getColumn());
                    else
                        assert false :  "Column not related to table";
                }
            }
        }
        return columnNames;
    }

    private @Nullable TriplesMapContext getContext(@Nonnull String table,
                                                   @Nullable Collection<?> columns) {
        Collection<String> columnNames = asColumnNames(table, columns);
        TriplesMapContext selected = null;
        int selectedColumns = -1, selectedSubset = -1, candidates = 0;
        for (TriplesMapContext ctx : table2contexts.get(table)) {
            Set<String> ctxColumns = ctx.getColumnNames();
            int subset = (int)columnNames.stream().filter(ctxColumns::contains).count();
            if (subset > selectedSubset || ctxColumns.size() > selectedColumns) {
                if (strict && selectedSubset == columnNames.size()) {
                    throw new RRMappingException("Table "+table+" matches multiple TriplesMaps " +
                                                 "with columns "+columnNames);
                } else {
                    ++candidates;
                    selected = ctx;
                    selectedSubset = subset;
                    selectedColumns = ctxColumns.size();
                }
            }
        }
        if (selectedColumns < columnNames.size() && selected != null) {
            logger.warn("No table mapping from table {} in {} contains these columns: {}. " +
                        "Selected {}, since it has the largest subset among {} candidates",
                        table, this, columnNames, selected.getRoot(), candidates);
        }

        return selected;
    }

    private int addTriples(@Nonnull Model m, @Nonnull String table,
                           @Nonnull Map<String, RDFNode> col2value) {
        int triples = 0, ctxCount = 0;
        for (TriplesMapContext ctx : table2contexts.get(table)) {
            ++ctxCount;
            triples += ctx.toRDF(m, col2value, strict, baseURI);
        }
        if (ctxCount == 0) {
            logger.warn("No TriplesMap found for table {} in {}.", table, this);
            if (strict)
                throw new RRMappingException("No TriplesMap for table "+table);
        }
        if (triples == 0) {
            logger.warn("No triples output from table {} in {} with assignments: {}",
                        table, this, col2value);
        }
        return triples;
    }

    /* --- --- --- Interface implementation --- --- --- */

    @Override
    public @Nonnull Molecule createMolecule(@Nullable Map<String, List<String>> table2columns) {
        if (strict && table2columns != null && !table2columns.isEmpty()) {
            throw new UnsupportedOperationException("RRMapping does not support overriding " +
                                                    "tables & columns at createMolecule");
        }
        if (this.molecule == null) {
            AtomNameSelector nameSelector = new AtomNameSelector(table2contexts.values());
            MoleculeBuilder builder = null;
            Map<String, TriplesMapContext> name2ctx = new TreeMap<>();
            for (TriplesMapContext ctx : table2contexts.values())
                name2ctx.put(nameSelector.get(ctx), ctx);
            for (Map.Entry<String, TriplesMapContext> e : name2ctx.entrySet()) {
                if (builder == null)
                    builder = Molecule.builder(e.getKey());
                else
                    builder.startNewCore(e.getKey());
                e.getValue().fillMolecule(builder, nameSelector);
            }
            if (table2contexts.isEmpty()) {
                if (strict)
                    throw new RRException("No rr:TriplesMap in R2RML");
                builder = Molecule.builder(""); //fallback for non-strict. Won't match
            }
            assert builder != null;
            this.molecule = builder.build();
        }
        return this.molecule;
    }

    @Override
    public @Nonnull Collection<String> getIdColumnsNames(@Nonnull String table,
                                                         @Nullable Collection<?> columns) {
        TriplesMapContext ctx = getContext(table, columns);
        if (ctx == null) {
            logger.warn("No TriplesMap for table {} in {}", table, this);
            if (strict)
                throw new RRMappingException("No table named "+table+" with columns "+columns);
            return Collections.emptyList();
        }
        return ctx.getSubject().getColumnNames();
    }

    @Override
    public @Nonnull Term getNameFor(@Nonnull Map<Column, Object> values) {
        String table = getTable("getNameFor", logger, values.keySet());
        if (table == null) //fallback if not killed by Exception/assert
            return JenaWrappers.fromJena(ResourceFactory.createResource());
        Map<String, Object> col2values = new HashMap<>(values.size());
        for (Map.Entry<Column, Object> e : values.entrySet())
            col2values.put(e.getKey().getColumn(), e.getValue());
        return getNameFor(table, col2values);
    }

    @Override
    public @Nonnull Term getNameFor(@Nonnull String table, @Nonnull Map<String, Object> values) {
        TriplesMapContext ctx = getContext(table, values.keySet());
        if (ctx == null) {
            logger.warn("No TriplesMap for table {} on {}", table, this);
            if (strict) throw new RRMappingException("No TriplesMapContext for table");
            return JenaWrappers.fromJena(ResourceFactory.createResource());
        }
        RDFNode node = ctx.getSubject().createTerm(values, sqlTermParser, baseURI);
        if (node == null) {
            throw new RRMappingException("Could not create RDF subject for table "+table+
                                         " with assignments "+values);
        }
        return JenaWrappers.fromJena(node);
    }

    @Override
    public int toRDF(@Nonnull Model model, @Nonnull List<Column> columns, @Nonnull List<?> values) {
        Preconditions.checkArgument(columns.size() == values.size(), "#columns != #values");
        assert columns.stream().noneMatch(Objects::isNull) : "null columns";
        if (columns.isEmpty()) {
            model.createResource(OWL2.Thing);
            return 1;
        }
        Map<String, Map<String, RDFNode>> maps = new HashMap<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Map<String, RDFNode> map = maps.computeIfAbsent(c.getTable(), k -> new HashMap<>());
            RDFNode old = map.put(c.getColumn(), sqlTermParser.parseNode(values.get(i)));
            if (old != null) {
                logger.warn("Ambiguous column {} in toRDF(Model, List, List). this={}", c, this);
                if (strict)
                    throw new RRMappingException("Column "+c+", appears twice in toRDF()");
            }
        }
        int total = 0;
        for (Map.Entry<String, Map<String, RDFNode>> e : maps.entrySet())
            total += addTriples(model, e.getKey(), e.getValue());
        return total;
    }

    @Override
    public int toRDF(@Nonnull Model m, @Nonnull String table, @Nonnull Map<String, Object> c2v) {
        Map<String, RDFNode> c2n = Maps.newHashMapWithExpectedSize(c2v.size());
        for (Map.Entry<String, Object> e : c2v.entrySet())
            c2n.put(e.getKey(), sqlTermParser.parseNode(e.getValue()));
        return addTriples(m, table, c2n);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder("RRMapping{");
        if (name != null) {
            b.append("name=").append(name);
        } else {
            for (TriplesMapContext map : table2contexts.values())
                b.append(map).append(", ");
            if (!table2contexts.isEmpty()) b.setLength(b.length() - 2);
        }
        return b.append('}').toString();
    }
}
