package br.ufsc.lapesd.riefederator.query;

import com.google.inject.ProvidedBy;

import java.util.function.BinaryOperator;


@ProvidedBy(RelativeCardinalityAdder.SingletonProvider.class)
public interface CardinalityAdder extends BinaryOperator<Cardinality> {

}
