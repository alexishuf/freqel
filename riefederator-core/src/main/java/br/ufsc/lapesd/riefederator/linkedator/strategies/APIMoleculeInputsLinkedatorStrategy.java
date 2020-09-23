package br.ufsc.lapesd.riefederator.linkedator.strategies;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.linkedator.LinkedatorResult;
import br.ufsc.lapesd.riefederator.linkedator.strategies.impl.AtomSignature;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.model.term.std.TemplateLink;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.SimplePath;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.APIMoleculeMatcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.vocabulary.OWL2;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.riefederator.linkedator.strategies.impl.AtomSignature.createInputSignatures;
import static br.ufsc.lapesd.riefederator.linkedator.strategies.impl.AtomSignature.createOutputSignatures;

public class APIMoleculeInputsLinkedatorStrategy implements LinkedatorStrategy {
    private static final Pattern URI_SPLIT_FRAGMENT_RX = Pattern.compile("^(.*#)([^#]+)$");
    private static final Pattern URI_SPLIT_PATH_RX = Pattern.compile("^(.*/)([^/#]+)$");
    private static final Pattern URN_SPLIT_RX = Pattern.compile("^(urn:.*:)([^:]+)$");
    private static final StdVar src = new StdVar("src"), dst = new StdVar("dst");
    private static final StdVar srcAnchor = new StdVar("srcAnchor"),
                                dstAnchor = new StdVar("dstAnchor");

    private static @Nonnull StdVar joinVar(int idx) {
        return new StdVar("linkedatorJoin"+idx);
    }

    @VisibleForTesting
    @Nonnull LinkedatorResult createSuggestion(@Nonnull AtomSignature inSig,
                                               @Nonnull AtomSignature outSig) {
        String relation;
        if (outSig.isDirectAnchor()) {
            relation = OWL2.sameAs.getURI();
        } else {
            // figure out better URI prefix
            Molecule.Index index = outSig.getMolecule().getIndex();
            Set<String> uris = outSig.getAtoms().stream()
                    .flatMap(a -> index.stream(null, null, a)
                                       .map(Molecule.Triple::getEdge).filter(Term::isURI)
                                       .map(t -> t.asURI().getURI()))
                    .collect(Collectors.toSet());
            String localName = "has" + StringUtils.capitalize(localName(inSig.getAnchor()));
            relation = findMostCommonPrefix(uris) + localName;
        }

        /* assert joinability through atoms */
        assert inSig.getAtoms().containsAll(outSig.getAtoms());
        assert outSig.getAtoms().containsAll(inSig.getRequiredAtoms());
        assert !outSig.getAtoms().isEmpty();

        IndexSet<String> joinAtoms = FullIndexSet.from(outSig.getAtoms());
        MutableCQuery query = new MutableCQuery();
        Var effSrc = outSig.getAnchorPath().isEmpty() ? src : srcAnchor;
        if (!outSig.getAnchorPath().isEmpty())
            query.add(src, outSig.getAnchorPath(), srcAnchor);
        for (String a : joinAtoms) {
            SimplePath path = outSig.getAtomPaths().get(a);
            assert path != null;
            assert !path.isEmpty();
            query.add(effSrc, path, joinVar(joinAtoms.indexOf(a)));
        }
        Var effDst = inSig.getAnchorPath().isEmpty() ? dst : dstAnchor;
        if (!inSig.getAnchorPath().isEmpty())
            query.add(dst, inSig.getAnchorPath(), dstAnchor);
        for (String a : joinAtoms)
            query.add(effDst, inSig.getAtomPaths().get(a), joinVar(joinAtoms.indexOf(a)));
        TemplateLink templateLink = new TemplateLink(relation, query, src, dst);

        return new LinkedatorResult(templateLink, this, 1);
    }

    @VisibleForTesting
    static @Nonnull String localName(@Nonnull String uri) {
        Matcher matcher = URI_SPLIT_FRAGMENT_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(2);
        matcher = URI_SPLIT_PATH_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(2);
        matcher = URN_SPLIT_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(2);
        return uri;
    }

    @VisibleForTesting
    static @Nonnull String prefix(@Nonnull String uri) {
        Matcher matcher = URI_SPLIT_FRAGMENT_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(1);
        matcher = URI_SPLIT_PATH_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(1);
        matcher = URN_SPLIT_RX.matcher(uri);
        if (matcher.matches())
            return matcher.group(1);
        return StdPlain.URI_PREFIX;
    }

    @VisibleForTesting
    static @Nonnull String findMostCommonPrefix(@Nonnull Set<String> uris) {
        assert uris.stream().noneMatch(Objects::isNull);
        HashMultiset<String> prefixes = HashMultiset.create(uris.size());
        for (String uri : uris) prefixes.add(prefix(uri));

        return prefixes.stream().max(Comparator.comparing(prefixes::count))
                       .orElse(StdPlain.URI_PREFIX);
    }

    @Override
    public @Nonnull Collection<LinkedatorResult> getSuggestions(@Nonnull Collection<Source> sources) {
        Set<LinkedatorResult> results = new HashSet<>();
        for (Source source : sources) {
            if (!(source.getDescription() instanceof APIMoleculeMatcher))
                continue;
            APIMolecule apiMol = ((APIMoleculeMatcher) source.getDescription()).getApiMolecule();
            if (apiMol.getMolecule().getCores().size() > 1)
                continue; //only support single-core molecules1
            for (AtomSignature inSig : createInputSignatures(apiMol)) {
                for (Source other : sources) {
                    if (other != source) {
                        Molecule mol;
                        if (other.getDescription() instanceof Molecule)
                            mol = (Molecule) other.getDescription();
                        else if (other.getDescription() instanceof APIMoleculeMatcher)
                            mol = ((APIMoleculeMatcher) other.getDescription()).getMolecule();
                        else
                            continue; //try another source
                        for (AtomSignature outSig : createOutputSignatures(inSig, mol))
                            results.add(createSuggestion(inSig, outSig));
                    }
                }
            }
        }
        return results;
    }
}
