package br.ufsc.lapesd.freqel.query.endpoint.impl;

import br.ufsc.lapesd.freqel.query.endpoint.DisjunctiveProfile;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;

import javax.annotation.Nonnull;
import java.util.Objects;

public class SPARQLDisjunctiveProfile implements DisjunctiveProfile {
    public static final @Nonnull SPARQLDisjunctiveProfile DEFAULT = new SPARQLDisjunctiveProfile();

    private boolean forbidUnion = false;
    private boolean forbidJoin = false;
    private boolean forbidProduct = false;
    private boolean forbidUnfilteredProduct = false;
    private boolean forbidUnfilteredProductRoot = true;
    private boolean forbidFilter = false;
    private boolean forbidOptional = false;
    private boolean forbidUnassignedQuery = false;

    public @Nonnull SPARQLDisjunctiveProfile forbidUnion() {
        this.forbidUnion = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidJoin() {
        this.forbidJoin = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidProduct() {
        this.forbidProduct = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidUnfilteredProduct() {
        this.forbidUnfilteredProduct = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidUnfilteredProductRoot() {
        this.forbidUnfilteredProductRoot = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidFilter() {
        this.forbidFilter = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidOptional() {
        this.forbidOptional = true;
        return this;
    }
    public @Nonnull SPARQLDisjunctiveProfile forbidUnassignedQuery() {
        this.forbidUnassignedQuery = true;
        return this;
    }

    @Override
    public boolean allowsUnion() {
        return !forbidUnion;
    }

    @Override
    public boolean allowsJoin() {
        return !forbidJoin;
    }

    @Override
    public boolean allowsProduct() {
        return !forbidProduct;
    }

    @Override
    public boolean allowsUnfilteredProduct() {
        return !forbidUnfilteredProduct;
    }

    @Override
    public boolean allowsUnfilteredProductRoot() {
        return !forbidUnfilteredProductRoot;
    }

    @Override
    public boolean allowsFilter(@Nonnull SPARQLFilter filter) {
        return !forbidFilter;
    }

    @Override
    public boolean allowsOptional() {
        return !forbidOptional;
    }

    @Override
    public boolean allowsUnassignedQuery() {
        return !forbidUnassignedQuery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SPARQLDisjunctiveProfile)) return false;
        SPARQLDisjunctiveProfile that = (SPARQLDisjunctiveProfile) o;
        return forbidUnion == that.forbidUnion &&
                forbidJoin == that.forbidJoin &&
                forbidProduct == that.forbidProduct &&
                forbidUnfilteredProduct == that.forbidUnfilteredProduct &&
                forbidUnfilteredProductRoot == that.forbidUnfilteredProductRoot &&
                forbidFilter == that.forbidFilter &&
                forbidOptional == that.forbidOptional &&
                forbidUnassignedQuery == that.forbidUnassignedQuery;
    }

    @Override
    public int hashCode() {
        return Objects.hash(forbidUnion, forbidJoin, forbidProduct, forbidUnfilteredProduct,
                            forbidUnfilteredProductRoot, forbidFilter, forbidOptional,
                            forbidUnassignedQuery);
    }
}
