package br.ufsc.lapesd.freqel.reason.regimes.subsumption;

import br.ufsc.lapesd.freqel.reason.regimes.EntailmentRegime;
import br.ufsc.lapesd.freqel.reason.rules.RuleSet;

import javax.annotation.Nonnull;

public class W3CSubsumptionChecker implements EntailmentRegimeSubsumptionChecker {
    private static final @Nonnull String NS = "http://www.w3.org/ns/entailment/";

    @Override
    public Boolean subsumes(@Nonnull EntailmentRegime subsumer,
                            @Nonnull EntailmentRegime subsumed) {
        String gt = subsumer.iri(), lt = subsumed.iri();
        if (lt.equals(NS+"Simple"))
            return true; // Simple is the universal "bottom" regime
        String rif = NS + "RIF";
        if (gt.equals(rif) && lt.equals(rif)) {
            RuleSet gtRules = subsumer.rules(), ltRules = subsumed.rules();
            if (gtRules == null && ltRules == null)
                return true; // both rules are unknown
            else if (gtRules == null || ltRules == null)
                return null; // rules unknown
            return gtRules.subsumes(ltRules);
        }
        if (!gt.startsWith(NS) || !lt.startsWith(NS))
            return null; // one of the IRIs is non-W3C
        if (gt.equals(lt))
            return true; // same IRI means equivalent

        if (lt.equals(NS+"RDF")) // RDF is subsumed by all but D
            return !gt.equals(NS+"D");
        if (lt.equals(NS+"RDFS")) // RDFS is subsumed by all but D and RDF
            return !gt.equals(NS+"RDF") && !gt.equals(NS+"D");
        if (lt.equals(NS+"D"))  // D-Entailment is subsumed only by OWL* in W3C's regimes
            return gt.startsWith(NS+"OWL");
        return false;
    }
}
