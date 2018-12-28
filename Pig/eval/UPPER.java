package com.pig.eval;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class UPPER extends EvalFunc<String> {

    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0 || tuple.get(0) == null) {
            return null;
        }

        String str = (String)tuple.get(0);
        return str.toUpperCase();
    }
}
