package com.pig.aggregate;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.util.Map;

public class COUNT extends EvalFunc<Long> implements Algebraic {

    public Long exec(Tuple tuple) throws ExecException {
        return count(tuple);
    }

    public String getInitial() {
        return org.apache.pig.builtin.COUNT.Initial.class.getName();
    }
    public String getIntermed() {
        return org.apache.pig.builtin.COUNT.Intermediate.class.getName();
    }
    public String getFinal() {
        return org.apache.pig.builtin.COUNT.Final.class.getName();
    }

    static protected Long count(Tuple tuple) throws ExecException {
        Object values = tuple.get(0);

        if (values instanceof DataBag)
            return ((DataBag)values).size();
        else if (values instanceof Map)
            return new Long(((Map)values).size());
        else
            return (long)0;
    }
}
