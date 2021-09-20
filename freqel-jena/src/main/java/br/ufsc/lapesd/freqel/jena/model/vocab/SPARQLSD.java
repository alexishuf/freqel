package br.ufsc.lapesd.freqel.jena.model.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import javax.annotation.Nonnull;

@SuppressWarnings("unused")
public class SPARQLSD {
    public static final @Nonnull String NS = "http://www.w3.org/ns/sparql-service-description#";


    /* --- --- --- Properties --- --- --- */

    public static final @Nonnull Property endpoint = ResourceFactory.createProperty(NS +"endpoint");
    public static final @Nonnull Property feature = ResourceFactory.createProperty(NS +"feature");
    public static final @Nonnull Property defaultEntailmentRegime = ResourceFactory.createProperty(NS +"defaultEntailmentRegime");
    public static final @Nonnull Property entailmentRegime = ResourceFactory.createProperty(NS +"entailmentRegime");
    public static final @Nonnull Property defaultSupportedEntailmentProfile = ResourceFactory.createProperty(NS +"defaultSupportedEntailmentProfile");
    public static final @Nonnull Property supportedEntailmentProfile = ResourceFactory.createProperty(NS +"supportedEntailmentProfile");
    public static final @Nonnull Property extensionFunction = ResourceFactory.createProperty(NS +"extensionFunction");
    public static final @Nonnull Property extensionAggregate = ResourceFactory.createProperty(NS +"extensionAggregate");
    public static final @Nonnull Property languageExtension = ResourceFactory.createProperty(NS +"languageExtension");
    public static final @Nonnull Property supportedLanguage = ResourceFactory.createProperty(NS +"supportedLanguage");
    public static final @Nonnull Property propertyFeature = ResourceFactory.createProperty(NS +"propertyFeature");
    public static final @Nonnull Property defaultDataset = ResourceFactory.createProperty(NS +"defaultDataset");
    public static final @Nonnull Property availableGraphs = ResourceFactory.createProperty(NS +"availableGraphs");
    public static final @Nonnull Property resultFormat = ResourceFactory.createProperty(NS +"resultFormat");
    public static final @Nonnull Property inputFormat = ResourceFactory.createProperty(NS +"inputFormat");
    public static final @Nonnull Property defaultGraph = ResourceFactory.createProperty(NS +"defaultGraph");
    public static final @Nonnull Property namedGraph = ResourceFactory.createProperty(NS +"namedGraph");
    public static final @Nonnull Property name = ResourceFactory.createProperty(NS +"name");
    public static final @Nonnull Property graph = ResourceFactory.createProperty(NS +"graph");


    /* --- --- --- Classes --- --- --- */

    public static final @Nonnull Resource Service = ResourceFactory.createResource(NS +"Service");
    public static final @Nonnull Resource Feature = ResourceFactory.createResource(NS +"Feature");
    public static final @Nonnull Resource Language = ResourceFactory.createResource(NS +"Language");
    public static final @Nonnull Resource Function = ResourceFactory.createResource(NS +"Function");
    public static final @Nonnull Resource Aggregate = ResourceFactory.createResource(NS +"Aggregate");
    public static final @Nonnull Resource EntailmentRegime = ResourceFactory.createResource(NS +"EntailmentRegime");
    public static final @Nonnull Resource EntailmentProfile = ResourceFactory.createResource(NS +"EntailmentProfile");
    public static final @Nonnull Resource GraphCollection = ResourceFactory.createResource(NS +"GraphCollection");
    public static final @Nonnull Resource Dataset = ResourceFactory.createResource(NS +"Dataset");
    public static final @Nonnull Resource Graph = ResourceFactory.createResource(NS +"Graph");
    public static final @Nonnull Resource NamedGraph = ResourceFactory.createResource(NS +"NamedGraph");


    /* --- --- --- Instances --- --- --- */

    public static final @Nonnull Resource SPARQL10Query = ResourceFactory.createResource(NS +"SPARQL10Query,");
    public static final @Nonnull Resource SPARQL11Query = ResourceFactory.createResource(NS +"SPARQL11Query,");
    public static final @Nonnull Resource SPARQL11Update = ResourceFactory.createResource(NS +"SPARQL11Update,");
    public static final @Nonnull Resource DereferencesURIs = ResourceFactory.createResource(NS +"DereferencesURIs,");
    public static final @Nonnull Resource UnionDefaultGraph = ResourceFactory.createResource(NS +"UnionDefaultGraph,");
    public static final @Nonnull Resource RequiresDataset = ResourceFactory.createResource(NS +"RequiresDataset,");
    public static final @Nonnull Resource EmptyGraphs = ResourceFactory.createResource(NS +"EmptyGraphs,");
    public static final @Nonnull Resource BasicFederatedQuery = ResourceFactory.createResource(NS +"BasicFederatedQuery");


    /* --- --- --- Result formats --- --- --- */

    public static final @Nonnull Resource XML_RESULTS = ResourceFactory.createResource("http://www.w3.org/ns/formats/SPARQL_Results_XML");
    public static final @Nonnull Resource JSON_RESULTS = ResourceFactory.createResource("http://www.w3.org/ns/formats/SPARQL_Results_JSON");
    public static final @Nonnull Resource CSV_RESULTS = ResourceFactory.createResource("http://www.w3.org/ns/formats/SPARQL_Results_CSV");
    public static final @Nonnull Resource TSV_RESULTS = ResourceFactory.createResource("http://www.w3.org/ns/formats/SPARQL_Results_TSV");
}
