package br.ufsc.lapesd.riefederator.query.parse.impl;

public class ConvertOptions {
    private boolean eraseOptionals = false;
    private boolean eraseGroupBy = false;
    private boolean eraseOffset = false;
    private boolean eraseOrderBy = false;
    private boolean allowExtraProjections = false;

    public boolean getEraseOptionals() {
        return eraseOptionals;
    }

    public void setEraseOptionals(boolean eraseOptionals) {
        this.eraseOptionals = eraseOptionals;
    }

    public boolean getEraseGroupBy() {
        return eraseGroupBy;
    }

    public void setEraseGroupBy(boolean eraseGroupBy) {
        this.eraseGroupBy = eraseGroupBy;
    }

    public boolean getEraseOffset() {
        return eraseOffset;
    }

    public void setEraseOffset(boolean eraseOffset) {
        this.eraseOffset = eraseOffset;
    }

    public boolean getEraseOrderBy() {
        return eraseOrderBy;
    }

    public void setEraseOrderBy(boolean eraseOrderBy) {
        this.eraseOrderBy = eraseOrderBy;
    }

    public boolean getAllowExtraProjections() {
        return allowExtraProjections;
    }

    public void setAllowExtraProjections(boolean allowExtraProjections) {
        this.allowExtraProjections = allowExtraProjections;
    }
}
