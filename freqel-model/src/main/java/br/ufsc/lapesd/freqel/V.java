package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;

@SuppressWarnings("unused")
public class V {
    public static class Freqel {
        public static final String NS = "http://alexishuf.github.io/freqel/";
        public static final StdURI evidences = new StdURI(NS+"evidences");

        public static class Entailment {
            public static final String NS = Freqel.NS+"entailment/";
            public static final StdURI evidences = new StdURI(NS+"evidences");

            public static class Graph {
                public static final String NS = Entailment.NS + "graph/";

                public static final StdURI Simple = new StdURI(NS+W3CEntailmentRegimes.SIMPLE.name());
                public static final StdURI RDF = new StdURI(NS+W3CEntailmentRegimes.RDF.name());
                public static final StdURI RDFS = new StdURI(NS+W3CEntailmentRegimes.RDFS.name());
                public static final StdURI D = new StdURI(NS+W3CEntailmentRegimes.D.name());
                public static final StdURI OWL_DIRECT = new StdURI(NS+W3CEntailmentRegimes.OWL_DIRECT.name());
                public static final StdURI OWL_RDF_BASED = new StdURI(NS+W3CEntailmentRegimes.OWL_RDF_BASED.name());
                public static final StdURI RIF = new StdURI(NS+W3CEntailmentRegimes.RIF.name());
            }
        }
    }

    public static class RDF {
        public static final String ONTOLOGY_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns";
        public static final String NS = ONTOLOGY_IRI+"#";

        public static final StdURI Alt          = new StdURI(NS+"Alt");
        public static final StdURI Bag          = new StdURI(NS+"Bag");
        public static final StdURI Property     = new StdURI(NS+"Property");
        public static final StdURI Seq          = new StdURI(NS+"Seq");
        public static final StdURI Statement    = new StdURI(NS+"Statement");
        public static final StdURI List         = new StdURI(NS+"List");
        public static final StdURI nil          = new StdURI(NS+"nil");
        public static final StdURI first        = new StdURI(NS+"first");
        public static final StdURI rest         = new StdURI(NS+"rest");
        public static final StdURI subject      = new StdURI(NS+"subject");
        public static final StdURI predicate    = new StdURI(NS+"predicate");
        public static final StdURI object       = new StdURI(NS+"object");
        public static final StdURI type         = new StdURI(NS+"type");
        public static final StdURI value        = new StdURI(NS+"value");
        public static final StdURI langString   = new StdURI(NS+"langString");
        public static final StdURI HTML         = new StdURI(NS+"HTML");
        public static final StdURI xmlLiteral   = new StdURI(NS+"xmlLiteral");
    }

    public static class RDFS {
        public static final String ONTOLOGY_IRI = "http://www.w3.org/2000/01/rdf-schema";
        public static final String NS = ONTOLOGY_IRI+"#";

        public static final StdURI Class          = new StdURI(NS+"Class");
        public static final StdURI Datatype       = new StdURI(NS+"Datatype");
        public static final StdURI Container      = new StdURI(NS+"Container");
        public static final StdURI ContainerMembershipProperty = new StdURI(NS+"ContainerMembershipProperty");
        public static final StdURI Literal        = new StdURI(NS+"Literal");
        public static final StdURI Resource       = new StdURI(NS+"Resource");
        public static final StdURI comment        = new StdURI(NS+"comment");
        public static final StdURI domain         = new StdURI(NS+"domain");
        public static final StdURI label          = new StdURI(NS+"label");
        public static final StdURI isDefinedBy    = new StdURI(NS+"isDefinedBy");
        public static final StdURI range          = new StdURI(NS+"range");
        public static final StdURI seeAlso        = new StdURI(NS+"seeAlso");
        public static final StdURI subClassOf     = new StdURI(NS+"subClassOf");
        public static final StdURI subPropertyOf  = new StdURI(NS+"subPropertyOf");
        public static final StdURI member         = new StdURI(NS+"member");
    }

    public static class OWL {
        public static final String ONTOLOGY_IRI = "http://www.w3.org/2002/07/owl";
        public static final String NS = ONTOLOGY_IRI+"#";

        public static final StdURI maxCardinality = new StdURI(NS+"maxCardinality");
        public static final StdURI versionInfo = new StdURI(NS+"versionInfo");
        public static final StdURI equivalentClass = new StdURI(NS+"equivalentClass");
        public static final StdURI distinctMembers = new StdURI(NS+"distinctMembers");
        public static final StdURI oneOf = new StdURI(NS+"oneOf");
        public static final StdURI incompatibleWith = new StdURI(NS+"incompatibleWith");
        public static final StdURI minCardinality = new StdURI(NS+"minCardinality");
        public static final StdURI complementOf = new StdURI(NS+"complementOf");
        public static final StdURI onProperty = new StdURI(NS+"onProperty");
        public static final StdURI equivalentProperty = new StdURI(NS+"equivalentProperty");
        public static final StdURI inverseOf = new StdURI(NS+"inverseOf");
        public static final StdURI backwardCompatibleWith = new StdURI(NS+"backwardCompatibleWith");
        public static final StdURI priorVersion = new StdURI(NS+"priorVersion");
        public static final StdURI imports = new StdURI(NS+"imports");
        public static final StdURI allValuesFrom = new StdURI(NS+"allValuesFrom");
        public static final StdURI unionOf = new StdURI(NS+"unionOf");
        public static final StdURI hasValue = new StdURI(NS+"hasValue");
        public static final StdURI someValuesFrom = new StdURI(NS+"someValuesFrom");
        public static final StdURI disjointWith = new StdURI(NS+"disjointWith");
        public static final StdURI cardinality = new StdURI(NS+"cardinality");
        public static final StdURI intersectionOf = new StdURI(NS+"intersectionOf");
        public static final StdURI sameAs = new StdURI(NS+"sameAs");
        public static final StdURI differentFrom = new StdURI(NS+"differentFrom");
        public static final StdURI topDataProperty = new StdURI(NS+"topDataProperty");
        public static final StdURI topObjectProperty = new StdURI(NS+"topObjectProperty");
        public static final StdURI bottomDataProperty = new StdURI(NS+"bottomDataProperty");
        public static final StdURI bottomObjectProperty = new StdURI(NS+"bottomObjectProperty");
        public static final StdURI Thing = new StdURI(NS+"Thing");
        public static final StdURI DataRange = new StdURI(NS+"DataRange");
        public static final StdURI Ontology = new StdURI(NS+"Ontology");
        public static final StdURI DeprecatedClass = new StdURI(NS+"DeprecatedClass");
        public static final StdURI AllDifferent = new StdURI(NS+"AllDifferent");
        public static final StdURI DatatypeProperty = new StdURI(NS+"DatatypeProperty");
        public static final StdURI SymmetricProperty = new StdURI(NS+"SymmetricProperty");
        public static final StdURI TransitiveProperty = new StdURI(NS+"TransitiveProperty");
        public static final StdURI DeprecatedProperty = new StdURI(NS+"DeprecatedProperty");
        public static final StdURI AnnotationProperty = new StdURI(NS+"AnnotationProperty");
        public static final StdURI Restriction = new StdURI(NS+"Restriction");
        public static final StdURI Class = new StdURI(NS+"Class");
        public static final StdURI OntologyProperty = new StdURI(NS+"OntologyProperty");
        public static final StdURI ObjectProperty = new StdURI(NS+"ObjectProperty");
        public static final StdURI FunctionalProperty = new StdURI(NS+"FunctionalProperty");
        public static final StdURI InverseFunctionalProperty = new StdURI(NS+"InverseFunctionalProperty");
        public static final StdURI Nothing = new StdURI(NS+"Nothing");
    }

    public static class XSD {
        public static final String NS = "http://www.w3.org/2001/XMLSchema#";

        public static StdURI xfloat = new StdURI(NS+"float");
        public static StdURI xdouble = new StdURI(NS+"double");
        public static StdURI xint = new StdURI(NS+"int");
        public static StdURI xlong = new StdURI(NS+"long");
        public static StdURI xshort = new StdURI(NS+"short");
        public static StdURI xbyte = new StdURI(NS+"byte");
        public static StdURI xboolean = new StdURI(NS+"boolean");
        public static StdURI xstring = new StdURI(NS+"string");
        public static StdURI unsignedByte = new StdURI(NS+"unsignedByte");
        public static StdURI unsignedShort = new StdURI(NS+"unsignedShort");
        public static StdURI unsignedInt = new StdURI(NS+"unsignedInt");
        public static StdURI unsignedLong = new StdURI(NS+"unsignedLong");
        public static StdURI decimal = new StdURI(NS+"decimal");
        public static StdURI integer = new StdURI(NS+"integer");
        public static StdURI nonPositiveInteger = new StdURI(NS+"nonPositiveInteger");
        public static StdURI nonNegativeInteger = new StdURI(NS+"nonNegativeInteger");
        public static StdURI positiveInteger = new StdURI(NS+"positiveInteger");
        public static StdURI negativeInteger = new StdURI(NS+"negativeInteger");
        public static StdURI normalizedString = new StdURI(NS+"normalizedString");
        public static StdURI anyURI = new StdURI(NS+"anyURI");
        public static StdURI token = new StdURI(NS+"token");
        public static StdURI Name = new StdURI(NS+"Name");
        public static StdURI QName = new StdURI(NS+"QName");
        public static StdURI language = new StdURI(NS+"language");
        public static StdURI NMTOKEN = new StdURI(NS+"NMTOKEN");
        public static StdURI ENTITIES = new StdURI(NS+"ENTITIES");
        public static StdURI NMTOKENS = new StdURI(NS+"NMTOKENS");
        public static StdURI ENTITY = new StdURI(NS+"ENTITY");
        public static StdURI ID = new StdURI(NS+"ID");
        public static StdURI NCName = new StdURI(NS+"NCName");
        public static StdURI IDREF = new StdURI(NS+"IDREF");
        public static StdURI IDREFS = new StdURI(NS+"IDREFS");
        public static StdURI NOTATION = new StdURI(NS+"NOTATION");
        public static StdURI hexBinary = new StdURI(NS+"hexBinary");
        public static StdURI base64Binary = new StdURI(NS+"base64Binary");
        public static StdURI date = new StdURI(NS+"date");
        public static StdURI time = new StdURI(NS+"time");
        public static StdURI dateTime = new StdURI(NS+"dateTime");
        public static StdURI dateTimeStamp = new StdURI(NS+"dateTimeStamp");
        public static StdURI duration = new StdURI(NS+"duration");
        public static StdURI yearMonthDuration = new StdURI(NS+"yearMonthDuration");
        public static StdURI dayTimeDuration = new StdURI(NS+"dayTimeDuration");
        public static StdURI gDay = new StdURI(NS+"gDay");
        public static StdURI gMonth = new StdURI(NS+"gMonth");
        public static StdURI gYear = new StdURI(NS+"gYear");
        public static StdURI gYearMonth = new StdURI(NS+"gYearMonth");
        public static StdURI gMonthDay = new StdURI(NS+"gMonthDay");
    }

    public static class FOAF {
        public static final String NS = "http://xmlns.com/foaf/0.1/";

        public static final StdURI account = new StdURI(NS+"account" );
        public static final StdURI accountName = new StdURI(NS+"accountName" );
        public static final StdURI accountServiceHomepage = new StdURI(NS+"accountServiceHomepage" );
        public static final StdURI age = new StdURI(NS+"age" );
        public static final StdURI aimChatID = new StdURI(NS+"aimChatID" );
        public static final StdURI based_near = new StdURI(NS+"based_near" );
        public static final StdURI birthday = new StdURI(NS+"birthday" );
        public static final StdURI currentProject = new StdURI(NS+"currentProject" );
        public static final StdURI depiction = new StdURI(NS+"depiction" );
        public static final StdURI depicts = new StdURI(NS+"depicts" );
        public static final StdURI dnaChecksum = new StdURI(NS+"dnaChecksum" );
        public static final StdURI familyName = new StdURI(NS+"familyName" );
        public static final StdURI family_name = new StdURI(NS+"family_name" );
        public static final StdURI firstName = new StdURI(NS+"firstName" );
        public static final StdURI focus = new StdURI(NS+"focus" );
        public static final StdURI fundedBy = new StdURI(NS+"fundedBy" );
        public static final StdURI geekcode = new StdURI(NS+"geekcode" );
        public static final StdURI gender = new StdURI(NS+"gender" );
        public static final StdURI givenName = new StdURI(NS+"givenName" );
        public static final StdURI givenname = new StdURI(NS+"givenname" );
        public static final StdURI holdsAccount = new StdURI(NS+"holdsAccount" );
        public static final StdURI homepage = new StdURI(NS+"homepage" );
        public static final StdURI icqChatID = new StdURI(NS+"icqChatID" );
        public static final StdURI img = new StdURI(NS+"img" );
        public static final StdURI interest = new StdURI(NS+"interest" );
        public static final StdURI isPrimaryTopicOf = new StdURI(NS+"isPrimaryTopicOf" );
        public static final StdURI jabberID = new StdURI(NS+"jabberID" );
        public static final StdURI knows = new StdURI(NS+"knows" );
        public static final StdURI lastName = new StdURI(NS+"lastName" );
        public static final StdURI logo = new StdURI(NS+"logo" );
        public static final StdURI made = new StdURI(NS+"made" );
        public static final StdURI maker = new StdURI(NS+"maker" );
        public static final StdURI mbox = new StdURI(NS+"mbox" );
        public static final StdURI mbox_sha1sum = new StdURI(NS+"mbox_sha1sum" );
        public static final StdURI member = new StdURI(NS+"member" );
        public static final StdURI membershipClass = new StdURI(NS+"membershipClass" );
        public static final StdURI msnChatID = new StdURI(NS+"msnChatID" );
        public static final StdURI myersBriggs = new StdURI(NS+"myersBriggs" );
        public static final StdURI name = new StdURI(NS+"name" );
        public static final StdURI nick = new StdURI(NS+"nick" );
        public static final StdURI openid = new StdURI(NS+"openid" );
        public static final StdURI page = new StdURI(NS+"page" );
        public static final StdURI pastProject = new StdURI(NS+"pastProject" );
        public static final StdURI phone = new StdURI(NS+"phone" );
        public static final StdURI plan = new StdURI(NS+"plan" );
        public static final StdURI primaryTopic = new StdURI(NS+"primaryTopic" );
        public static final StdURI publications = new StdURI(NS+"publications" );
        public static final StdURI schoolHomepage = new StdURI(NS+"schoolHomepage" );
        public static final StdURI sha1 = new StdURI(NS+"sha1" );
        public static final StdURI skypeID = new StdURI(NS+"skypeID" );
        public static final StdURI status = new StdURI(NS+"status" );
        public static final StdURI surname = new StdURI(NS+"surname" );
        public static final StdURI theme = new StdURI(NS+"theme" );
        public static final StdURI thumbnail = new StdURI(NS+"thumbnail" );
        public static final StdURI tipjar = new StdURI(NS+"tipjar" );
        public static final StdURI title = new StdURI(NS+"title" );
        public static final StdURI topic = new StdURI(NS+"topic" );
        public static final StdURI topic_interest = new StdURI(NS+"topic_interest" );
        public static final StdURI weblog = new StdURI(NS+"weblog" );
        public static final StdURI workInfoHomepage = new StdURI(NS+"workInfoHomepage" );
        public static final StdURI workplaceHomepage = new StdURI(NS+"workplaceHomepage" );
        public static final StdURI yahooChatID = new StdURI(NS+"yahooChatID" );
        public static final StdURI Agent = new StdURI(NS+"Agent" );
        public static final StdURI Document = new StdURI(NS+"Document" );
        public static final StdURI Group = new StdURI(NS+"Group" );
        public static final StdURI Image = new StdURI(NS+"Image" );
        public static final StdURI LabelProperty = new StdURI(NS+"LabelProperty" );
        public static final StdURI OnlineAccount = new StdURI(NS+"OnlineAccount" );
        public static final StdURI OnlineChatAccount = new StdURI(NS+"OnlineChatAccount" );
        public static final StdURI OnlineEcommerceAccount = new StdURI(NS+"OnlineEcommerceAccount" );
        public static final StdURI OnlineGamingAccount = new StdURI(NS+"OnlineGamingAccount" );
        public static final StdURI Organization = new StdURI(NS+"Organization" );
        public static final StdURI Person = new StdURI(NS+"Person" );
        public static final StdURI PersonalProfileDocument = new StdURI(NS+"PersonalProfileDocument" );
        public static final StdURI Project = new StdURI(NS+"Project" );
    }
    
    public static class DCT {
        public static final String NS = "http://purl.org/dc/terms/";

        public static final StdURI abstract_ = new StdURI(NS+"abstract" );
        public static final StdURI accessRights = new StdURI(NS+"accessRights" );
        public static final StdURI accrualMethod = new StdURI(NS+"accrualMethod" );
        public static final StdURI accrualPeriodicity = new StdURI(NS+"accrualPeriodicity" );
        public static final StdURI accrualPolicy = new StdURI(NS+"accrualPolicy" );
        public static final StdURI alternative = new StdURI(NS+"alternative" );
        public static final StdURI audience = new StdURI(NS+"audience" );
        public static final StdURI available = new StdURI(NS+"available" );
        public static final StdURI bibliographicCitation = new StdURI(NS+"bibliographicCitation" );
        public static final StdURI conformsTo = new StdURI(NS+"conformsTo" );
        public static final StdURI contributor = new StdURI(NS+"contributor" );
        public static final StdURI coverage = new StdURI(NS+"coverage" );
        public static final StdURI created = new StdURI(NS+"created" );
        public static final StdURI creator = new StdURI(NS+"creator" );
        public static final StdURI date = new StdURI(NS+"date" );
        public static final StdURI dateAccepted = new StdURI(NS+"dateAccepted" );
        public static final StdURI dateCopyrighted = new StdURI(NS+"dateCopyrighted" );
        public static final StdURI dateSubmitted = new StdURI(NS+"dateSubmitted" );
        public static final StdURI description = new StdURI(NS+"description" );
        public static final StdURI educationLevel = new StdURI(NS+"educationLevel" );
        public static final StdURI extent = new StdURI(NS+"extent" );
        public static final StdURI format = new StdURI(NS+"format" );
        public static final StdURI hasFormat = new StdURI(NS+"hasFormat" );
        public static final StdURI hasPart = new StdURI(NS+"hasPart" );
        public static final StdURI hasVersion = new StdURI(NS+"hasVersion" );
        public static final StdURI identifier = new StdURI(NS+"identifier" );
        public static final StdURI instructionalMethod = new StdURI(NS+"instructionalMethod" );
        public static final StdURI isFormatOf = new StdURI(NS+"isFormatOf" );
        public static final StdURI isPartOf = new StdURI(NS+"isPartOf" );
        public static final StdURI isReferencedBy = new StdURI(NS+"isReferencedBy" );
        public static final StdURI isReplacedBy = new StdURI(NS+"isReplacedBy" );
        public static final StdURI isRequiredBy = new StdURI(NS+"isRequiredBy" );
        public static final StdURI isVersionOf = new StdURI(NS+"isVersionOf" );
        public static final StdURI issued = new StdURI(NS+"issued" );
        public static final StdURI language = new StdURI(NS+"language" );
        public static final StdURI license = new StdURI(NS+"license" );
        public static final StdURI mediator = new StdURI(NS+"mediator" );
        public static final StdURI medium = new StdURI(NS+"medium" );
        public static final StdURI modified = new StdURI(NS+"modified" );
        public static final StdURI provenance = new StdURI(NS+"provenance" );
        public static final StdURI publisher = new StdURI(NS+"publisher" );
        public static final StdURI references = new StdURI(NS+"references" );
        public static final StdURI relation = new StdURI(NS+"relation" );
        public static final StdURI replaces = new StdURI(NS+"replaces" );
        public static final StdURI requires = new StdURI(NS+"requires" );
        public static final StdURI rights = new StdURI(NS+"rights" );
        public static final StdURI rightsHolder = new StdURI(NS+"rightsHolder" );
        public static final StdURI source = new StdURI(NS+"source" );
        public static final StdURI spatial = new StdURI(NS+"spatial" );
        public static final StdURI subject = new StdURI(NS+"subject" );
        public static final StdURI tableOfContents = new StdURI(NS+"tableOfContents" );
        public static final StdURI temporal = new StdURI(NS+"temporal" );
        public static final StdURI title = new StdURI(NS+"title" );
        public static final StdURI type = new StdURI(NS+"type" );
        public static final StdURI valid = new StdURI(NS+"valid" );
        public static final StdURI Agent = new StdURI(NS+"Agent" );
        public static final StdURI AgentClass = new StdURI(NS+"AgentClass" );
        public static final StdURI BibliographicResource = new StdURI(NS+"BibliographicResource" );
        public static final StdURI FileFormat = new StdURI(NS+"FileFormat" );
        public static final StdURI Frequency = new StdURI(NS+"Frequency" );
        public static final StdURI Jurisdiction = new StdURI(NS+"Jurisdiction" );
        public static final StdURI LicenseDocument = new StdURI(NS+"LicenseDocument" );
        public static final StdURI LinguisticSystem = new StdURI(NS+"LinguisticSystem" );
        public static final StdURI Location = new StdURI(NS+"Location" );
        public static final StdURI LocationPeriodOrJurisdiction = new StdURI(NS+"LocationPeriodOrJurisdiction" );
        public static final StdURI MediaType = new StdURI(NS+"MediaType" );
        public static final StdURI MediaTypeOrExtent = new StdURI(NS+"MediaTypeOrExtent" );
        public static final StdURI MethodOfAccrual = new StdURI(NS+"MethodOfAccrual" );
        public static final StdURI MethodOfInstruction = new StdURI(NS+"MethodOfInstruction" );
        public static final StdURI PeriodOfTime = new StdURI(NS+"PeriodOfTime" );
        public static final StdURI PhysicalMedium = new StdURI(NS+"PhysicalMedium" );
        public static final StdURI PhysicalResource = new StdURI(NS+"PhysicalResource" );
        public static final StdURI Policy = new StdURI(NS+"Policy" );
        public static final StdURI ProvenanceStatement = new StdURI(NS+"ProvenanceStatement" );
        public static final StdURI RightsStatement = new StdURI(NS+"RightsStatement" );
        public static final StdURI SizeOrDuration = new StdURI(NS+"SizeOrDuration" );
        public static final StdURI Standard = new StdURI(NS+"Standard" );
    }
}
