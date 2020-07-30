package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.riefederator.rel.common.RelationalTermParser;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RRTemplate;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.JoinCondition;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.ReferencingObjectMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.XSD;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TermContext {
    private @Nonnull final TermMap root;
    private @LazyInit @Nullable ImmutableList<String> columnNames;
    private final @Nullable TermContext parentCtx;
    private final @Nullable Map<String, String> parent2child;

    public TermContext(@Nonnull TermMap root) {
        this.root = root;
        if (root.canAs(ReferencingObjectMap.class)) {
            ReferencingObjectMap ref = root.as(ReferencingObjectMap.class);
            parentCtx = new TermContext(ref.getParentTriplesMap().getSubjectMap());
            parent2child = new LinkedHashMap<>();
            // this outer loop looks silly but is required to ensure keys ordering in parent2child
            for (String parentColumn : parentCtx.getColumnNames()) {
                for (JoinCondition c : ref.getJoinConditions()) {
                    if (Objects.equals(c.getParent(), parentColumn)) {
                        String old = parent2child.put(c.getParent(), c.getChild());
                        if (old != null && !old.equals(c.getParent())) {
                            throw new InvalidRRException(root, RR.joinCondition, "Target (parent) " +
                                    "column "+c.getParent()+" already mapped from "+old);
                        }
                    }
                }
            }
            if (parent2child.isEmpty()) {
                // rr:joinConditions are not required when rr:parentTriplesMap
                // yields the same effective SQL query as the referencing triples map.
                for (String column : parentCtx.getColumnNames())
                    parent2child.put(column, column);
            }
        } else {
            parentCtx = null;
            parent2child = null;
        }
    }

    public @Nonnull TermMap getRoot() {
        return root;
    }

    public boolean isReference() {
        return parentCtx != null;
    }

    public boolean isConstant(@Nonnull RDFNode expected) {
        return root.getConstant() != null && root.getConstant().equals(expected);
    }

    public @Nonnull ImmutableList<String> getColumnNames() {
        if (columnNames == null) {
            if (root.getColumn() != null) {
                columnNames = ImmutableList.of(root.getColumn());
            } else if (root.getTemplate() != null) {
                columnNames = root.getTemplate().getOrderedColumnNames();
            } else if (root.getConstant() != null) {
                columnNames = ImmutableList.of();
            } else {
                assert parent2child != null : "No rr:{column,template,constant,parentTriplesMap}";
                columnNames = ImmutableList.copyOf(parent2child.values());
            }
        }
        return columnNames;
    }

    public boolean isDirect() {
        if (root.getColumn() != null)
            return true;
        if (parentCtx != null)
            return parentCtx.isDirect();
        return false;
    }

    private @Nullable RDFNode validateAndFixNode(@Nonnull RDFNode node) {
        TermType type = root.getTermType();
        String lang = root.getLanguage();
        Resource dtResource = root.getDatatype();
        if (type.accepts(node)) {
            if (type == TermType.Literal) {
                Literal lit = node.asLiteral();
                if (dtResource == null)
                    return node; // non-specified datatype for the mapping
                if (lang != null && lang.equals(lit.getLanguage()))
                    return node; // is a lang literal and language tag matches
                if (Objects.equals(dtResource.getURI(), lit.getDatatypeURI()))
                    return node; // typed literal and type matches
                // in case of divergences rebuild the literal below
            } else {
                return node;
            }
        }
        // rebuild the result node
        String lex = node.isLiteral() ? node.asLiteral().getLexicalForm() : node.toString();
        if (type == TermType.Literal) {
            if (lang != null)
                return ResourceFactory.createLangLiteral(lex, lang);
            if (dtResource == null) dtResource = XSD.xstring;
            RDFDatatype dt = TypeMapper.getInstance().getSafeTypeByName(dtResource.getURI());
            return ResourceFactory.createTypedLiteral(lex, dt);
        } else if (type == TermType.IRI) {
            return ResourceFactory.createResource(lex);
        } else if (type == TermType.BlankNode) {
            return ResourceFactory.createResource();
        }
        throw new UnsupportedOperationException("No support for "+type);
    }

    private @Nullable RDFNode expandReference(@Nonnull Map<String, ?> childCol2value,
                                              @Nonnull RelationalTermParser parser,
                                              @Nonnull String baseURI) {
        assert parentCtx != null;
        assert parent2child != null;
        Map<String, Object> translated = Maps.newHashMapWithExpectedSize(childCol2value.size());
        for (Map.Entry<String, String> e : parent2child.entrySet())
            translated.put(e.getKey(), childCol2value.getOrDefault(e.getValue(), null));
        return parentCtx.createTerm(translated, parser, baseURI);
    }

    public @Nullable RDFNode createTerm(@Nonnull Map<String, ?> col2value,
                                        @Nonnull RelationalTermParser parser) {
        return createTerm(col2value, parser, "");
    }

    public @Nullable RDFNode createTerm(@Nonnull Map<String, ?> col2value,
                                        @Nonnull RelationalTermParser parser,
                                        @Nonnull String baseURI) {
        // as per test case http://www.w3.org/2001/sw/rdb2rdf/test-cases/#tc001b,
        // termType takes precedence
        if (root.getTermType() == TermType.BlankNode)
            return ResourceFactory.createResource();
        String column = root.getColumn();
        if (column != null) {
            TermType type = root.getTermType();
            RDFNode node = parser.parseNode(col2value.get(column));
            if (node == null)
                return null;
            else if (type.accepts(node) && type != TermType.Literal)
                return node;
            else
                return validateAndFixNode(node);
        }
        RDFNode constant = root.getConstant();
        if (constant != null)
            return constant;
        RRTemplate template = root.getTemplate();
        if (template == null)
            return expandReference(col2value, parser, baseURI);
        return template.tryExpand(col2value, root, parser, baseURI);
    }
}
