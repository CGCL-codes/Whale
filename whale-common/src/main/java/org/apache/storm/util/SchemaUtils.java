package org.apache.storm.util;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class SchemaUtils
{
    public static List<String> parseSchema(String schemaDesp) {
        final Splitter splitter = Splitter.on(',').trimResults();

        return ImmutableList.copyOf(splitter.split(schemaDesp));
    }

    public static int getFieldIdx(String schemaDesp, String field) {
        List<String> schema = parseSchema(schemaDesp);

        int idx = schema.indexOf(field.trim());

        checkState(idx >= 0, "No \"" + field + "\" in " + schema);
        return idx;
    }
}
