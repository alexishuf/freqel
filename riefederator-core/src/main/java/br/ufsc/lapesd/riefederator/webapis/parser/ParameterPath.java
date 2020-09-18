package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.AtomFilter;
import br.ufsc.lapesd.riefederator.description.molecules.AtomRole;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class ParameterPath {
    private static final Logger logger = LoggerFactory.getLogger(ParameterPath.class);

    private final @Nonnull Atom atom;
    private final @Nonnull List<Term> path;
    private final boolean in;
    private final boolean missing;
    private final @Nullable String naValue;
    private final @Nonnull Collection<AtomFilter> atomFilters;

    private static final @Nonnull String inVar = "input", acVar = "actual";
    private static final @Nonnull Set<String> FILTER_VARS = Sets.newHashSet(inVar, acVar);

    public ParameterPath(@Nonnull Atom atom, @Nonnull List<Term> path, boolean in,
                         boolean missing, @Nullable String naValue,
                         @Nonnull Collection<AtomFilter> filters) {
        checkArgument(!path.isEmpty(), "Path cannot be empty");
        checkArgument(path.stream().noneMatch(Objects::isNull), "Path cannot have null steps");
        this.atom = atom;
        this.path = path;
        this.in = in;
        this.missing = missing;
        this.naValue = naValue;
        this.atomFilters = filters;
    }

    public @Nonnull Atom getAtom() {
        return atom;
    }

    public @Nonnull List<Term> getPath() {
        return path;
    }

    public boolean isIn() {
        return in;
    }

    public boolean isMissingInResult() {
        return missing;
    }

    public @Nullable String getNaValue() {
        return naValue;
    }

    public @Nonnull Collection<AtomFilter> getAtomFilters() {
        return atomFilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterPath)) return false;
        ParameterPath that = (ParameterPath) o;
        return in == that.in &&
                missing == that.missing &&
                atom.equals(that.atom) &&
                path.equals(that.path) &&
                Objects.equals(atomFilters, that.atomFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(atom, path, in, missing, atomFilters);
    }

    public static @Nonnull ParameterPath parse(@Nonnull DictTree xPath,
                                               @Nonnull Molecule molecule,
                                               @Nonnull Function<String, Term> property2Term) {
        StringBuilder builder = new StringBuilder();
        ParameterPath parameterPath = tryParse(xPath, molecule, property2Term, builder);
        if (parameterPath == null)
            throw new IllegalArgumentException(builder.toString());
        return parameterPath;
    }

    public static @Nullable ParameterPath tryParse(@Nonnull DictTree xPath,
                                                   @Nonnull Molecule molecule,
                                                   @Nonnull Function<String, Term> property2Term,
                                                   @Nullable StringBuilder errorMsg) {
        List<Object> rawPath = xPath.getListNN("path");
        if (rawPath.isEmpty()) {
            if (errorMsg != null)
                errorMsg.append("path array is empty");
            return null;
        }
        if (rawPath.stream().anyMatch(Objects::isNull)) {
            if (errorMsg != null)
                errorMsg.append("There are null values in the path array");
            return null;
        }
        List<Term> path = xPath.getListNN("path").stream().filter(Objects::nonNull)
                .map(Object::toString).map(property2Term).collect(Collectors.toList());
        if (path.stream().anyMatch(Objects::isNull)) {
            if (errorMsg != null) {
                errorMsg.append("Some properties could not be converted to terms: ");
                for (int i = 0; i < path.size(); i++) {
                    if (path.get(i) == null)
                        errorMsg.append(rawPath.get(i)).append(", ");
                }
                errorMsg.setLength(errorMsg.length()-2);
            }
            return null;
        }
        boolean in = xPath.contains("direction", "in");
        boolean missing = xPath.contains("missing", true);
        Atom atom = getAtom(molecule, path, in);
        if (atom == null) {
            if (errorMsg != null)
                errorMsg.append("Path ").append(path).append(" not present in molecule");
            return null;
        }
        String naValue = "";
        if (xPath.containsKey("na-value")) {
            Object value = xPath.get("na-value");
            naValue = value == null ? null : value.toString();
        }
        return new ParameterPath(atom, path, in, missing, naValue, parseFilters(atom, xPath, errorMsg));
    }

    @VisibleForTesting
    static @Nullable Atom getAtom(@Nonnull Molecule molecule, @Nonnull List<Term> path,
                                  boolean in) {
        if (in) {
            path = new ArrayList<>(path);
            Collections.reverse(path);
        }
        Molecule.Index index = molecule.getIndex();
        List<Atom> atoms = new ArrayList<>();
        ArrayDeque<ImmutablePair<String, Integer>> stack = new ArrayDeque<>();
        stack.push(ImmutablePair.of(molecule.getCore().getName(), 0));
        while (!stack.isEmpty()) {
            ImmutablePair<String, Integer> s = stack.pop();
            if (s.right == path.size()) {
                Atom atom = molecule.getAtom(s.left);
                assert atom != null;
                atoms.add(atom);
                continue;
            }
            assert s.right < path.size();
            assert s.left != null;
            int nIdx = s.right + 1;
            index.stream(in ? null : s.left, path.get(s.right), in ? s.left : null)
                    .forEach(t -> stack.push(ImmutablePair.of(in ? t.getSubj():t.getObj(), nIdx)));
        }
        assert atoms.stream().noneMatch(Objects::isNull);
        if (atoms.size() > 1) {
            logger.warn("Ambiguous {} path {} leads to two atoms from core atom {}. " +
                        "Will use the the first result, {}", in ? "in" : "out",
                    path, molecule.getCore().getName(), atoms.get(0).getName());
        }
        return atoms.isEmpty() ? null : atoms.get(0);
    }

    @VisibleForTesting
    static @Nonnull List<AtomFilter> parseFilters(@Nonnull Atom atom, @Nonnull DictTree xPath,
                                                    @Nullable StringBuilder errorMsg) {
        if (!xPath.containsKey("filter"))
            return Collections.emptyList();
        List<AtomFilter> parsed = new ArrayList<>();
        for (Object elem : xPath.getListNN("filter")) {
            AtomFilter filter = null;
            if (elem instanceof String) {
                filter = parseFilter(atom, (String)elem, errorMsg);
            } else if (elem instanceof DictTree) {
                filter = parseFilter(atom, (DictTree)elem, errorMsg);
            } else if (errorMsg != null) {
                errorMsg.append("Value of filter is neither object nor string, will ignore.");
            }
            if (filter != null) parsed.add(filter);
        }
        return parsed;
    }


    static @Nullable AtomFilter parseFilter(@Nonnull Atom atom, @Nullable String filterString,
                                            @Nullable StringBuilder errorMsg) {
        return parseFilter(atom, filterString, errorMsg, MIN_VALUE, true);
    }

    static @Nullable AtomFilter parseFilter(@Nonnull Atom atom, @Nullable String filterString,
                                            @Nullable StringBuilder errorMsg, int inputIndex,
                                            boolean required) {
        if (filterString == null) return null; // no value

        SPARQLFilter filter = SPARQLFilter.build(filterString);
        if (!filter.getVarNames().equals(FILTER_VARS)) {
            if (errorMsg != null) {
                errorMsg.append("Filter has unexpected vars. Expected input and actual. Found: ")
                        .append(filter.getVarNames());
            }
            return null;
        }
        AtomFilter.Builder builder = AtomFilter.builder(filter);
        if (filter.getVarNames().contains(inVar))
            builder.map(AtomRole.INPUT.wrap(atom), inVar);
        if (filter.getVarNames().contains(acVar))
            builder.map(AtomRole.OUTPUT.wrap(atom), acVar);
        if (inputIndex != MIN_VALUE)
            builder.withInputIndex(inputIndex);
        builder.required(required);
        return builder.build();
    }

    @VisibleForTesting
    static @Nullable AtomFilter parseFilter(@Nonnull Atom atom, @Nonnull DictTree map,
                                            @Nullable StringBuilder errorMsg) {
        if (!map.containsKey("sparql")) {
            if (errorMsg != null)
                errorMsg.append("Only SPARQL filters are supported. Got ").append(map.keySet());
            return null;
        }
        int index = (int)min(max(map.getLong("index", MIN_VALUE), MIN_VALUE), MAX_VALUE);
        boolean required = map.getBoolean("required", true);
        Object sparql = map.get("sparql");
        if (sparql != null) {
            if (!(sparql instanceof String)) {
                if (errorMsg != null)
                    errorMsg.append("Value of sparql filter is not a string: ").append(sparql);
                return null;
            }
            return parseFilter(atom, (String) sparql, errorMsg, index, required);
        }
        return null;
    }

}
