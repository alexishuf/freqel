package br.ufsc.lapesd.riefederator.rel.mappings.r2rml;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.URI;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import javax.annotation.Nonnull;

public class RR {
    public static final String NS = "http://www.w3.org/ns/r2rml#";

    /* --- --- --- Jena Resource instances --- --- --- */

    public static final Property logicalTable = ResourceFactory.createProperty(NS+"logicalTable");
    public static final Property subjectMap = ResourceFactory.createProperty(NS+"subjectMap");
    public static final Property template = ResourceFactory.createProperty(NS+"template");
    public static final Property rrClass = ResourceFactory.createProperty(NS+"class");
    public static final Property predicateObjectMap = ResourceFactory.createProperty(NS+"predicateObjectMap");
    public static final Property predicate = ResourceFactory.createProperty(NS+"predicate");
    public static final Property predicateMap = ResourceFactory.createProperty(NS+"predicateMap");
    public static final Property object = ResourceFactory.createProperty(NS+"object");
    public static final Property objectMap = ResourceFactory.createProperty(NS+"objectMap");
    public static final Property column = ResourceFactory.createProperty(NS+"column");
    public static final Property tableName = ResourceFactory.createProperty(NS+"tableName");
    public static final Property parentTriplesMap = ResourceFactory.createProperty(NS+"parentTriplesMap");
    public static final Property joinCondition = ResourceFactory.createProperty(NS+"joinCondition");
    public static final Property child = ResourceFactory.createProperty(NS+"child");
    public static final Property parent = ResourceFactory.createProperty(NS+"parent");
    public static final Property sqlQuery = ResourceFactory.createProperty(NS+"sqlQuery");
    public static final Property sqlVersion = ResourceFactory.createProperty(NS+"sqlVersion");
    public static final Property constant = ResourceFactory.createProperty(NS+"constant");
    public static final Property termType = ResourceFactory.createProperty(NS+"termType");
    public static final Property language = ResourceFactory.createProperty(NS+"language");
    public static final Property datatype = ResourceFactory.createProperty(NS+"datatype");
    public static final Property inverseExpression = ResourceFactory.createProperty(NS+"inverseExpression");

    public static final Resource TriplesMap = ResourceFactory.createResource(NS+"TriplesMap");
    public static final Resource LogicalTable = ResourceFactory.createResource(NS+"LogicalTable");
    public static final Resource R2RMLView = ResourceFactory.createResource(NS+"R2RMLView");
    public static final Resource BaseTableOrView = ResourceFactory.createResource(NS+"BaseTableOrView");
    public static final Resource TermMap = ResourceFactory.createResource(NS+"TermMap");
    public static final Resource SubjectMap = ResourceFactory.createResource(NS+"SubjectMap");
    public static final Resource PredicateMap = ResourceFactory.createResource(NS+"PredicateMap");
    public static final Resource ObjectMap = ResourceFactory.createResource(NS+"ObjectMap");
    public static final Resource GraphMap = ResourceFactory.createResource(NS+"GraphMap");
    public static final Resource PredicateObjectMap = ResourceFactory.createResource(NS+"PredicateObjectMap");
    public static final Resource RefObjectMap = ResourceFactory.createResource(NS+"RefObjectMap");
    public static final Resource Join = ResourceFactory.createResource(NS+"Join");

    public static final Resource IRI = ResourceFactory.createResource(NS+"IRI");
    public static final Resource BlankNode = ResourceFactory.createResource(NS+"BlankNode");
    public static final Resource Literal = ResourceFactory.createResource(NS+"Literal");

    /* --- --- --- Term instances --- --- --- */

    public static final URI logicalTableTerm = JenaWrappers.fromURIResource(logicalTable);
    public static final URI subjectMapTerm = JenaWrappers.fromURIResource(subjectMap);
    public static final URI templateTerm = JenaWrappers.fromURIResource(template);
    public static final URI rrClassTerm = JenaWrappers.fromURIResource(rrClass);
    public static final URI predicateObjectMapTerm = JenaWrappers.fromURIResource(predicateObjectMap);
    public static final URI predicateTerm = JenaWrappers.fromURIResource(predicate);
    public static final URI predicateMapTerm = JenaWrappers.fromURIResource(predicateMap);
    public static final URI objectTerm = JenaWrappers.fromURIResource(object);
    public static final URI objectMapTerm = JenaWrappers.fromURIResource(objectMap);
    public static final URI columnTerm = JenaWrappers.fromURIResource(column);
    public static final URI tableNameTerm = JenaWrappers.fromURIResource(tableName);
    public static final URI parentTriplesMapTerm = JenaWrappers.fromURIResource(parentTriplesMap);
    public static final URI joinConditionTerm = JenaWrappers.fromURIResource(joinCondition);
    public static final URI childTerm = JenaWrappers.fromURIResource(child);
    public static final URI parentTerm = JenaWrappers.fromURIResource(parent);
    public static final URI sqlQueryTerm = JenaWrappers.fromURIResource(sqlQuery);
    public static final URI sqlVersionTerm = JenaWrappers.fromURIResource(sqlVersion);
    public static final URI constantTerm = JenaWrappers.fromURIResource(constant);
    public static final URI termTypeTerm = JenaWrappers.fromURIResource(termType);
    public static final URI languageTerm = JenaWrappers.fromURIResource(language);
    public static final URI datatypeTerm = JenaWrappers.fromURIResource(datatype);
    public static final URI inverseExpressionTerm = JenaWrappers.fromURIResource(inverseExpression);

    public static final URI TriplesMapTerm = JenaWrappers.fromURIResource(TriplesMap);
    public static final URI LogicalTableTerm = JenaWrappers.fromURIResource(LogicalTable);
    public static final URI R2RMLViewTerm = JenaWrappers.fromURIResource(R2RMLView);
    public static final URI BaseTableOrViewTerm = JenaWrappers.fromURIResource(BaseTableOrView);
    public static final URI TermMapTerm = JenaWrappers.fromURIResource(TermMap);
    public static final URI SubjectMapTerm = JenaWrappers.fromURIResource(SubjectMap);
    public static final URI PredicateMapTerm = JenaWrappers.fromURIResource(PredicateMap);
    public static final URI ObjectMapTerm = JenaWrappers.fromURIResource(ObjectMap);
    public static final URI GraphMapTerm = JenaWrappers.fromURIResource(GraphMap);
    public static final URI PredicateObjectMapTerm = JenaWrappers.fromURIResource(PredicateObjectMap);
    public static final URI RefObjectMapTerm = JenaWrappers.fromURIResource(RefObjectMap);
    public static final URI JoinTerm = JenaWrappers.fromURIResource(Join);

    public static final URI IRITerm = JenaWrappers.fromURIResource(IRI);
    public static final URI BlankNodeTerm = JenaWrappers.fromURIResource(BlankNode);
    public static final URI LiteralTerm = JenaWrappers.fromURIResource(Literal);


    /* --- --- --- Utilities --- --- --- */

    public static @Nonnull String toString(@Nonnull Resource resource) {
        if (resource.isURIResource())
            return resource.getURI().replace(NS, "");
        return resource.toString();
    }
}
