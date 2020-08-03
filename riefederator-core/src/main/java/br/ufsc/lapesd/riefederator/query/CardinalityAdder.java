package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.query.impl.RelativeCardinalityAdder;
import com.google.inject.ProvidedBy;

import java.util.function.BinaryOperator;


@ProvidedBy(RelativeCardinalityAdder.SingletonProvider.class)
public interface CardinalityAdder extends BinaryOperator<Cardinality> {

}
