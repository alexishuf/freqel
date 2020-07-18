package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.description.molecules.tags.HybridTag;
import br.ufsc.lapesd.riefederator.description.molecules.tags.ValueTag;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.*;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.RRMappingException;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.UnsupportedRRException;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.PostRelationalTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.rel.sql.impl.NaturalSqlTermParser;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static java.lang.String.format;

public class TriplesMapContext {
    private static final String EX_MSG_1 =
            "Predicate term %s created for table for %s from %s is not an IRI";
    private static final URI rdfType = JenaWrappers.fromURIResource(RDF.type);
    private static final Logger logger = LoggerFactory.getLogger(TriplesMapContext.class);

    private final TriplesMap root;
    private @Nonnull final String table;
    private @Nonnull final TermContext sCtx;
    private @Nonnull final List<PredicateObjectContext> pos;
    private @LazyInit @Nullable Set<String> columnsNames;
    private final boolean unique;

    public TriplesMapContext(@Nonnull TriplesMap root) {
        this.root = root;
        LogicalTable logicalTable = root.getLogicalTable();
        if (logicalTable.canAs(R2RMLView.class))
            throw new UnsupportedRRException("rr:R2RMLView (rr:sqlQuery) is not supported");
        table = logicalTable.as(BaseTableOrView.class).getTableName();
        sCtx = new TermContext(root.getSubjectMap());
        pos = root.streamPredicateObjectMap().map(PredicateObjectContext::new)
                                             .collect(Collectors.toList());
        unique = checkUnique();
    }

    private boolean checkUnique() {
        Model model = root.getModel();
        if (model == null)
            return true;
        String safe = table.replaceAll("(?<!\\\\)\"", "\\\\\"");
        String sparql = "PREFIX rr: <" + RR.NS + ">\n" +
                "SELECT ?s WHERE {?s rr:logicalTable/rr:tableName \"" + safe + "\".}";
        int count = 0;
        try (QueryExecution exec = QueryExecutionFactory.create(sparql, model)) {
            ResultSet rs = exec.execSelect();
            while (rs.hasNext() && count < 2) {
                rs.nextBinding();
                ++count;
            }
        }
        return count == 1;
    }

    public @Nonnull TriplesMap getRoot() {
        return root;
    }

    public @Nonnull String getTable() {
        return table;
    }

    public @Nonnull TermContext getSubject() {
        return sCtx;
    }

    public @Nonnull List<PredicateObjectContext> getPredicateObjects() {
        return pos;
    }

    public @Nonnull Set<String> getColumnNames() {
        if (columnsNames == null) {
            Set<String> set = new HashSet<>(getSubject().getColumnNames());
            for (PredicateObjectContext po : pos)
                set.addAll(po.getColumnNames());
            columnsNames = set;
        }
        return columnsNames;
    }

    private @Nullable
    ColumnsTag getColumnTag(@Nonnull TermContext object) {
        ImmutableList<String> names = object.getColumnNames();
        if (names.isEmpty())
            return null;
        ImmutableList.Builder<Column> b = ImmutableList.builder();
        for (String name : names)
            b.add(new Column(table, name));
        return new ColumnsTag(b.build(), object.isDirect());
    }

    private void fillMolecule(@Nonnull MoleculeBuilder builder,
                              @Nonnull AtomNameSelector selector,
                              @Nonnull Collection<HybridTag> typeColumnTags,
                              @Nonnull PredicateObjectPairContext pair) {
        RDFNode pNode = pair.getPredicate().getRoot().getConstant();
        if (pNode == null) {
            throw new UnsupportedRRException("Non-constant predicates are not supported");
        } else if (!pNode.isURIResource()) {
            throw new InvalidRRException(pair.getPredicate().getRoot(), RR.constant,
                    "Predicates must be IRIs");
        }
        URI p = JenaWrappers.fromURIResource(pNode.asResource());

        ColumnsTag columnsTag = getColumnTag(pair.getObject());
        if (p.equals(rdfType)) {
            if (columnsTag != null) typeColumnTags.add(columnsTag);
        } else {
            List<HybridTag> tags = new ArrayList<>();
            tags.add(columnsTag == null ? PostRelationalTag.INSTANCE : columnsTag);
            if (pair.getObject().isReference()) {
                TriplesMap tm = pair.getObject().getRoot().as(ReferencingObjectMap.class)
                        .getParentTriplesMap();
                TriplesMapContext tmCtx = new TriplesMapContext(tm);
                String atomName = selector.get(tmCtx);
                builder.tag(atomName, tags);
                builder.out(p, atomName, tags);
            } else {
                MoleculeBuilder oBuilder = Molecule.builder(selector.get(this, pair));
                tags.forEach(oBuilder::tag);
                builder.out(p, oBuilder.buildAtom(), tags);
            }
        }
    }

    public void fillMolecule(@Nonnull MoleculeBuilder builder,
                             @Nonnull AtomNameSelector selector) {
        builder.tag(new TableTag(table));
        ImmutableList<String> colNames = getSubject().getColumnNames();
        if (!colNames.isEmpty()) {
            ImmutableList.Builder<Column> listBuilder = ImmutableList.builder();
            for (String name : colNames)
                listBuilder.add(new Column(table, name));

            builder.tag(new ColumnsTag(listBuilder.build(), getSubject().isDirect()));
        }

        List<HybridTag> typeTags = new ArrayList<>();
        for (PredicateObjectContext po : pos) {
            for (PredicateObjectPairContext pair : po.getPairs()) {
                fillMolecule(builder, selector, typeTags, pair);
            }
        }
        if (root.streamClasses().findFirst().isPresent()) {
            MoleculeBuilder typeAtomBuilder = Molecule.builder(selector.get(this) + "-type");
            if (typeTags.isEmpty())
                typeTags.add(PostRelationalTag.INSTANCE);
            typeTags.forEach(typeAtomBuilder::tag);
            getRoot().streamClasses().forEach(c -> typeAtomBuilder.tag(new ValueTag(fromJena(c))));
            builder.out(rdfType, typeAtomBuilder.buildAtom(), typeTags);
        }
        builder.exclusive().closed();
    }

    public int toRDF(@Nonnull Model model, @Nonnull Map<String, RDFNode> c2n, boolean strict,
                     @Nonnull String baseURI) {
        NaturalSqlTermParser parser = NaturalSqlTermParser.INSTANCE;
        RDFNode subjNode = sCtx.createTerm(c2n, parser, baseURI);
        if (subjNode == null)
            return 0; // insufficient data, cannot create node
        Resource subj = subjNode.asResource();
        int triples = 0;
        for (PredicateObjectContext po : pos) {
            for (PredicateObjectPairContext pair : po.getPairs()) {
                RDFNode pn = pair.getPredicate().createTerm(c2n, parser, baseURI);
                if (pn == null) {
                    logger.warn("Could not create predicate for {} from {}", table, c2n);
                    continue;
                } else if (!pn.isURIResource()) {
                    logger.warn("Predicate {} for table {} from {} is not an IRI", pn, table, c2n);
                    if (strict)
                        throw new RRMappingException(format(EX_MSG_1, pn, table, c2n));
                    continue;
                }
                RDFNode on = pair.getObject().createTerm(c2n, parser, baseURI);
                if (on == null) {
                    logger.debug("Could not create object for {} in table {} from {}",
                                 pn, table, c2n);
                    continue;
                }
                model.add(subj, ResourceFactory.createProperty(pn.asResource().getURI()), on);
                ++triples;
            }
        }
        for (Resource aClass : root.getSubjectMap().getClasses()) {
            model.add(subj, RDF.type, aClass);
            ++triples;
        }
        if (triples == 0) {
            // add a not really useful triple so that the subject exists
            // this may happen when all objects of a subject are bound in the CQuery
            // and the goal is to obtain a solution for the subject.
            model.add(subj, RDF.type, OWL.Thing);
            triples = 1;
        }
        return triples;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TriplesMapContext)) return false;
        TriplesMapContext that = (TriplesMapContext) o;
        return Objects.equals(getRoot(), that.getRoot());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRoot());
    }

    @Override
    public @Nonnull String toString() {
        if (unique)
            return table;
        return table + getColumnNames();
    }
}
