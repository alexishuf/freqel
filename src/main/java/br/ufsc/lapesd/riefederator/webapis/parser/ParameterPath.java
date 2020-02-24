package br.ufsc.lapesd.riefederator.webapis.parser;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.util.DictTree;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class ParameterPath {
    private static final Logger logger = LoggerFactory.getLogger(ParameterPath.class);

    private final @Nonnull Atom atom;
    private final @Nonnull List<Term> path;
    private final boolean in;
    private final boolean missing;
    private final @Nullable String sparqlFilter;

    public ParameterPath(@Nonnull Atom atom, @Nonnull List<Term> path, boolean in,
                         boolean missing, @Nullable String sparqlFilter) {
        checkArgument(!path.isEmpty(), "Path cannot be empty");
        checkArgument(path.stream().noneMatch(Objects::isNull), "Path cannot have null steps");
        this.atom = atom;
        this.path = path;
        this.in = in;
        this.missing = missing;
        this.sparqlFilter = sparqlFilter;
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

    public boolean isMissing() {
        return missing;
    }

    public @Nullable String getSparqlFilter() {
        return sparqlFilter;
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
                Objects.equals(sparqlFilter, that.sparqlFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(atom, path, in, missing, sparqlFilter);
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
        String filter = xPath.containsKey("filter") ? parseFilter(xPath, errorMsg) : null;
        return new ParameterPath(atom, path, in, missing, filter);
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
                atoms.add(molecule.getAtomMap().get(s.left));
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
    static @Nullable String parseFilter(@Nonnull DictTree xPath, @Nullable StringBuilder errorMsg) {
        if (!xPath.containsKey("filter")) return null;
        DictTree map = xPath.getMapNN("filter");
        String string;
        if (map.isEmpty()) {
            Object value = xPath.getPrimitive("filter", null);
            string = value == null ? null : value.toString();
        } else {
            if (!map.containsKey("sparql")) {
                if (errorMsg != null)
                    errorMsg.append("Only SPARQL filters are supported. Got ").append(map.keySet());
                return null;
            }
            Object sparql = map.get("sparql");
            if (sparql != null && !(sparql instanceof String)) {
                if (errorMsg != null)
                    errorMsg.append("Value of sparql filter is not a string: ").append(sparql);
                return null;
            }
            string = (String) sparql;
        }

        String sparql = String.format("SELECT * WHERE { ?s ?p $actual. ?s ?p $input %s.}", string);
        try {
            QueryFactory.create(sparql);
        } catch (QueryException e) {
            if (errorMsg != null) {
                errorMsg.append("Syntax of ").append(string).append(" is invalid: ")
                        .append(e.getMessage());
            }
            return null;
        }
        return string;
    }

}
