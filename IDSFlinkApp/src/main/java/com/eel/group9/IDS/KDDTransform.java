package com.eel.group9.IDS;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.Locale;

public class KDDTransform implements MapFunction<String, String> {


    @Override
    public String map(String s) throws Exception {
        String output = "Message received at " + System.currentTimeMillis() + "\n";
        output += s.toUpperCase(Locale.ROOT);
        return output;
    }
}
