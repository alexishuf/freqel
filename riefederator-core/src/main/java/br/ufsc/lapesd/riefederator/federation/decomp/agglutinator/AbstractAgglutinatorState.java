package br.ufsc.lapesd.riefederator.federation.decomp.agglutinator;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import java.util.ArrayList;

public abstract class AbstractAgglutinatorState implements Agglutinator.State {
    protected CQuery query;
    protected IndexSet<Triple> triplesUniverse;
    protected IndexSet<String> varsUniverse;
    private int nTriples, nVars;

    protected AbstractAgglutinatorState(@Nonnull CQuery query) {
        setQuery(query);
    }

    protected void setQuery(@Nonnull CQuery query) {
        this.query = query;
        IndexSet<String> vars = query.attr().varNamesUniverseOffer();
        if (vars == null) {
            vars = query.attr().allVarNames();
            if (vars instanceof IndexSubset)
                vars = vars.getParent();
            vars = FullIndexSet.fromDistinctCopy(vars);
            query.attr().offerVarNamesUniverse(vars);
        }
        IndexSet<Triple> triples = query.attr().triplesUniverseOffer();
        if (triples == null) {
            triples = query.attr().getSet();
            if (triples instanceof IndexSubset)
                triples = triples.getParent();
            triples = FullIndexSet.fromDistinctCopy(triples);
            query.attr().offerTriplesUniverse(triples);
        }
        varsUniverse = vars;
        triplesUniverse = triples;
        nTriples = triplesUniverse.size();
        nVars = vars.size();
    }

    protected @Nonnull CQuery addUniverse(@Nonnull CQuery query) {
        query.attr().offerVarNamesUniverse(varsUniverse);
        query.attr().offerTriplesUniverse(triplesUniverse);
        return query;
    }

    protected @Nonnull CQuery toQuery(@Nonnull Bitset triples) {
        ArrayList<Triple> list = new ArrayList<>(triples.cardinality());
        for (int i = triples.nextSetBit(0); i >= 0; i = triples.nextSetBit(i+1))
            list.add(triplesUniverse.get(i));
        return addUniverse(MutableCQuery.from(list));
    }

    protected @Nonnull Bitset toSignature(@Nonnull CQuery query) {
        IndexSet<Triple> triples = query.attr().getSet();
        assert triples instanceof IndexSubset;

        Bitset sig = Bitsets.createFixed(nTriples + nVars);
        sig.or(((IndexSubset<Triple>) triples).getBitset());
        sig.or(nTriples, query.attr().inputVarNames().getBitset(), 0, nVars);
        return sig;
    }
}
