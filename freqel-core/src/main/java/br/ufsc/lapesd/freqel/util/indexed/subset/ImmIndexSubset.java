package br.ufsc.lapesd.freqel.util.indexed.subset;

import br.ufsc.lapesd.freqel.util.indexed.ImmIndexSet;
import com.google.errorprone.annotations.Immutable;

@Immutable
public interface ImmIndexSubset<T> extends IndexSubset<T>, ImmIndexSet<T> {
}
