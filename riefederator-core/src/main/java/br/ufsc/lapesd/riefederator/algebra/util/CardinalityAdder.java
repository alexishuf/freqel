package br.ufsc.lapesd.riefederator.algebra.util;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import com.google.inject.ProvidedBy;

import java.util.function.BinaryOperator;


@ProvidedBy(RelativeCardinalityAdder.SingletonProvider.class)
public interface CardinalityAdder extends BinaryOperator<Cardinality> {

}
