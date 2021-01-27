package br.ufsc.lapesd.freqel.webapis.parser;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeLink;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.util.DictTree;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.PrimitiveParser;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.PrimitiveParserParser;
import br.ufsc.lapesd.freqel.webapis.requests.parsers.impl.PrimitiveParsersRegistry;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class JsonSchemaMoleculeParser {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final Pattern defPayloadPropRx =
            Pattern.compile("data|payload|(member|item|value|result|element)s?");
    private static final Pattern defLinkPropRx = Pattern.compile("[_$]?(h?refs?|links?)");

    private boolean closed = true, exclusive = true;
    private @Nonnull Map<String, Term> prop2Term = Collections.emptyMap();
    private @Nonnull String defaultPrefix = StdPlain.URI_PREFIX;
    private @Nonnull Pattern payloadPropRx = defPayloadPropRx;
    private @Nonnull Pattern linkPropRx = defLinkPropRx;

    private @Nonnull Cardinality cardinality = Cardinality.UNSUPPORTED;
    private @Nullable Molecule molecule;
    private @Nonnull PrimitiveParsersRegistry parsersRegistry = new PrimitiveParsersRegistry();
    private @Nullable PrimitiveParser globalDateParser;

    /* --- --- --- --- configuration --- --- --- ---  */

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public void setProp2Term(@Nonnull Map<String, Term> prop2Term) {
        this.prop2Term = prop2Term;
    }

    public void setDefaultPrefix(@Nonnull String defaultPrefix) {
        this.defaultPrefix = defaultPrefix;
    }

    public void setPayloadPropRx(@Nonnull Pattern payloadPropRx) {
        this.payloadPropRx = payloadPropRx;
    }

    public void setLinkPropRx(@Nonnull Pattern linkPropRx) {
        this.linkPropRx = linkPropRx;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public @Nonnull Map<String, Term> getProp2Term() {
        return prop2Term;
    }

    public @Nonnull String getDefaultPrefix() {
        return defaultPrefix;
    }

    public @Nonnull Pattern getPayloadPropRx() {
        return payloadPropRx;
    }

    public @Nonnull Pattern getLinkPropRx() {
        return linkPropRx;
    }

    /* --- --- --- --- other results --- --- --- ---  */

    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    public @Nonnull Molecule getMolecule() {
        if (molecule == null)
            throw new NoSuchElementException();
        return molecule;
    }

    public @Nonnull PrimitiveParsersRegistry getParsersRegistry() {
        return parsersRegistry;
    }

    public @Nullable Atom getAtomForSchemaPath(List<String> path, boolean in) {
        Preconditions.checkState(molecule != null, "parse() must be called before!");
        Atom atom = molecule.getCore();
        for (String step : path) {
            Term e = prop2Term(step);
            atom = (in ? atom.getIn() : atom.getOut()).stream().filter(l -> l.getEdge().equals(e))
                    .map(MoleculeLink::getAtom).findFirst().orElse(null);
            if (atom == null) return null; // bad path
        }
        return atom;
    }

    /* --- --- --- --- parsing --- --- --- ---  */

    public void setGlobalDateParser(@Nullable PrimitiveParser parser) {
        globalDateParser = parser;
    }

    public @Nullable PrimitiveParser getGlobalDateParser() {
        return globalDateParser;
    }

    public @Nonnull Molecule parse(@Nonnull DictTree schemaRoot) {
        if (!schemaRoot.contains("type", "array")) {
            cardinality = Cardinality.upperBound(1);
        } else {
            long minItems = schemaRoot.getLong("minItems", 0);
            if (minItems == 0) // assume maxItems is maxItems **per page**
                minItems = schemaRoot.getLong("maxItems", 0);
            cardinality = Cardinality.lowerBound((int)Math.max(2, minItems));
            schemaRoot = schemaRoot.getMapNN("items");
        }
        parsersRegistry = new PrimitiveParsersRegistry();
        MoleculeBuilder builder = new MoleculeBuilder(nameForSchema(schemaRoot));
        molecule = feed(builder, schemaRoot, new ArrayList<>()).build();
        return molecule;
    }

    @Contract("_, _, _ -> param1")
    private @Nonnull MoleculeBuilder feed(@Nonnull MoleculeBuilder builder,
                                          @Nonnull DictTree schema, @Nonnull List<String> path) {
        DictTree map = schema.getMapNN("properties");
        // if an array, use the properties of the items
        if (map.isEmpty() && schema.contains("type", "array"))
            map = schema.getMapNN("items").getMapNN("properties");
        // add an outgoing edge for each property
        for (String prop : map.keySet()) {
            DictTree childSchema = map.getMapNN(prop);
            String childName = requireNonNull(childSchema.getName());
            MoleculeBuilder childBuilder = new MoleculeBuilder(childName);
            path.add(prop);
            builder.out(prop2Term(prop), feed(childBuilder, childSchema, path).buildAtom());
            PrimitiveParser parser = PrimitiveParserParser.parse(childSchema.getMap("x-parser"));
            if (parser != null)
                parsersRegistry.add(path, parser);
            else if (globalDateParser != null && childSchema.contains("format", "date"))
                parsersRegistry.add(path, globalDateParser);
            path.remove(path.size()-1);
        }
        // apply closed and exclusive
        builder.exclusive(isExclusive());
        builder.closed(isClosed());
        return builder;
    }

    public @Nonnull Term prop2Term(@Nonnull String prop) {
        Term term = prop2Term.getOrDefault(prop, null);
        if (term == null) {
            if (defaultPrefix.equals(StdPlain.URI_PREFIX))
                return new StdPlain(prop);
            return new StdURI(defaultPrefix + prop);
        }
        return term;
    }

    private static @Nonnull String nameForSchema(@Nonnull DictTree schemaRoot) {
        String name = schemaRoot.getName();
        return name == null ? "Anon-" + nextId.getAndIncrement() : name;
    }
}
