package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.rdf.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RRUtils {
    private static final Logger logger = LoggerFactory.getLogger(RRUtils.class);

    @SuppressWarnings("unchecked")
    public static <T extends RDFNode> T in(@Nullable T node, @Nullable Model model) {
        if (model == null || node == null) {
            return node;
        } else if (node.isURIResource()) {
            return (T)model.createResource(node.asResource().getURI());
        } else if (node.isResource()) {
            return (T)model.createResource(node.asResource().getId());
        } else {
            assert node.isLiteral();
            Literal lit = node.asLiteral();
            if (!lit.getLanguage().isEmpty())
                return (T)model.createLiteral(lit.getLexicalForm(), lit.getLanguage());
            if (lit.getDatatype() != null)
                return (T)model.createTypedLiteral(lit.getLexicalForm(), lit.getDatatype());
            else
                return (T)model.createLiteral(lit.getLexicalForm());
        }
    }

    public static @Nullable RDFNode getNode(@Nonnull Resource s, @Nonnull Property p) {
        StmtIterator it = s.getModel().listStatements(s, p, (RDFNode) null);
        if (it.hasNext()) {
            RDFNode node = it.next().getObject();
            if (it.hasNext())
                logger.warn("Chose {} for {} of {}, but there are more candidates!", node, p, s);
            return node;
        }
        return null;
    }

    public static @Nonnull RDFNode getNodeNN(@Nonnull Resource s, @Nonnull Property p) {
        RDFNode node = getNode(s, p);
        if (node == null)
            throw new InvalidRRException(s, p, "No object found");
        return node;
    }

    public static @Nullable String getString(@Nonnull Resource s, @Nonnull Property p) {
        return getString(s, p, false);
    }

    private static @Nullable String getString(@Nonnull Resource s, @Nonnull Property p,
                                              boolean intolerant) {
        StmtIterator it = s.getModel().listStatements(s, p, (RDFNode) null);
        Object value = null;
        while (it.hasNext()) {
            RDFNode o = it.next().getObject();
            if (o.isLiteral()) value = o.asLiteral().getValue();
            if (value instanceof String) {
                if (it.hasNext()) {
                    logger.warn("Returning {} for {} of {}, but there are more objects",
                                value, p, o);
                }
                return (String) value;
            } else {
                if (it.hasNext())
                    logger.warn("Expected a string for {} of subject {}, found {}", p, s, o);
                else if (intolerant)
                    throw new InvalidRRException(s, p, "Expected a string, got "+o);
            }
        }
        return null;
    }

    public static @Nonnull String getStringNN(@Nonnull Resource s, @Nonnull Property p) {
        String string = getString(s, p, true);
        if (string == null)
            throw new InvalidRRException(s, p, "No object is a string");
        return string;
    }

    public static @Nullable Resource getResource(@Nonnull Resource s, @Nonnull Property p) {
        return getResource(s, p, false);
    }

    private static @Nullable Resource getResource(@Nonnull Resource s, @Nonnull Property p,
                                                 boolean intolerant) {
        StmtIterator it = s.getModel().listStatements(s, p, (RDFNode) null);
        Resource value = null;
        while (it.hasNext()) {
            RDFNode o = it.next().getObject();
            if (o.isResource()) value = o.asResource();
            if (value != null) {
                return value;
            } else {
                if (it.hasNext())
                    logger.warn("Expected a resource for {} of subject {}, found {}", p, s, o);
                else if (intolerant)
                    throw new InvalidRRException(s, p, "Expected a resource, got "+o);
            }
        }
        return null;
    }

    public static @Nonnull Resource getResourceNN(@Nonnull Resource s, @Nonnull Property p) {
        Resource resource = getResource(s, p, true);
        if (resource == null)
            throw new InvalidRRException(s, p, "No resource");
        return resource;
    }

    public static @Nullable Resource getURIResource(@Nonnull Resource s, @Nonnull Property p) {
        return getURIResource(s, p, false);
    }

    private static @Nullable Resource getURIResource(@Nonnull Resource s, @Nonnull Property p,
                                                    boolean intolerant) {
        StmtIterator it = s.getModel().listStatements(s, p, (RDFNode) null);
        Resource value = null;
        while (it.hasNext()) {
            RDFNode o = it.next().getObject();
            if (o.isURIResource()) value = o.asResource();
            if (value != null) {
                return value;
            } else {
                if (it.hasNext())
                    logger.warn("Expected an URI resource for {} of subject {}, found {}", p, s, o);
                else if (intolerant)
                    throw new InvalidRRException(s, p, "Expected an URI resource, got "+o);
            }
        }
        return null;
    }

    public static @Nonnull Resource getURIResourceNN(@Nonnull Resource s, @Nonnull Property p) {
        Resource resource = getURIResource(s, p, true);
        if (resource == null)
            throw new InvalidRRException(s, p, "No object is an URI resource");
        return resource;
    }
}
