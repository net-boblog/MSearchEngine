package com.midea.cloudSearch.druid.segment;

public class Hint {
    private HintType type;
    private Object[] params;
    public Hint(HintType type,Object[] params) {
        this.type = type;
        this.params = params;
    }

    public HintType getType() {
        return type;
    }

    public Object[] getParams() {
        return params;
    }
}
