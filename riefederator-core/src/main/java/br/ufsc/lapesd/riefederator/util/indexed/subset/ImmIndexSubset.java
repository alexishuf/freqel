package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.indexed.ImmIndexSet;
import com.google.errorprone.annotations.Immutable;

@Immutable
public interface ImmIndexSubset<T> extends IndexSubset<T>, ImmIndexSet<T> {
}
