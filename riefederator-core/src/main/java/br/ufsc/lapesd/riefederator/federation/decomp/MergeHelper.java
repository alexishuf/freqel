package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.annotations.MergePolicyAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MergeHelper {
    public static @Nullable MergePolicyAnnotation getMergePolicy(@Nonnull CQuery query) {
        for (QueryAnnotation a : query.getQueryAnnotations()) {
            if (a instanceof MergePolicyAnnotation)
                return (MergePolicyAnnotation)a;
        }
        return null;
    }

    public static boolean canMerge(@Nonnull CQuery left, @Nonnull CQuery right) {
        MergePolicyAnnotation leftPolicy = getMergePolicy(left);
        MergePolicyAnnotation rightPolicy = getMergePolicy(right);
        if (leftPolicy != null && !leftPolicy.canMerge(left, right)) return false;
        return rightPolicy == null || rightPolicy.canMerge(right, left);
    }

    public static @Nullable CQuery tryMerge(@Nonnull CQuery left, @Nonnull CQuery right) {
        if (!canMerge(left, right))
            return null;
        MutableCQuery q = new MutableCQuery(left);
        q.mergeWith(right);
        return q;
    }
}