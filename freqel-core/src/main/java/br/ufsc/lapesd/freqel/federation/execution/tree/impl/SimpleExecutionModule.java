package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.DefaultJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.FixedBindJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.generators.SubTermReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.DescriptionReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.MultiReplacementPruner;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

import javax.annotation.Nonnull;

public class SimpleExecutionModule extends AbstractModule {
    protected boolean allowHashJoins = true;

    @CanIgnoreReturnValue
    public @Nonnull SimpleExecutionModule forceBindJoins() {
        allowHashJoins = false;
        return this;
    }

    @Override
    protected void configure() {
        configureResultsExecutor();
        bind(QueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(DQueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(UnionOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(CartesianOpExecutor.class).to(LazyCartesianOpExecutor.class);
        bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
        if (allowHashJoins)
            bind(JoinOpExecutor.class).to(DefaultJoinOpExecutor.class);
        else
            bind(JoinOpExecutor.class).to(FixedBindJoinOpExecutor.class);
        bind(EmptyOpExecutor.class).toInstance(SimpleEmptyOpExecutor.INSTANCE);
        bind(PipeOpExecutor.class).to(SimplePipeOpExecutor.class);
        bind(SPARQLValuesTemplateOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(PlanExecutor.class).to(InjectedExecutor.class);

        Multibinder<ReplacementPruner> prunerBinder =
                Multibinder.newSetBinder(binder(), ReplacementPruner.class);
        configureReplacementPrunersSet(prunerBinder);
        configureReplacementPruner();
        bind(ReplacementGenerator.class).to(SubTermReplacementGenerator.class);
    }

    private void configureReplacementPruner() {
        bind(ReplacementPruner.class).to(MultiReplacementPruner.class);
    }

    protected void configureReplacementPrunersSet(@Nonnull Multibinder<ReplacementPruner> binder) {
        binder.addBinding().to(DescriptionReplacementPruner.class);
    }

    protected void configureResultsExecutor() {
        bind(ResultsExecutor.class).toInstance(new BufferedResultsExecutor());
    }
}
