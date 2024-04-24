package com.eel.group9.IDS;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class KDDTransform implements MapFunction<String, String> {


    Map<String, Integer> embeddings;
    Map<Integer, Integer> scalingMap;

    public KDDTransform() {
        this.embeddings = new HashMap<>();
        this.scalingMap = new HashMap<>();

        embeddings.put("SF", 0);
        embeddings.put("S0", 1);
        embeddings.put("REJ", 2);
        embeddings.put("RSTR", 3);
        embeddings.put("RSTO", 4);
        embeddings.put("SH", 5);
        embeddings.put("S1", 6);
        embeddings.put("S2", 7);
        embeddings.put("RSTOS0", 8);
        embeddings.put("S3", 9);
        embeddings.put("OTH", 10);

        // New entries
        embeddings.put("icmp", 0);
        embeddings.put("tcp", 1);
        embeddings.put("udp", 2);

    }

    @Override
    public String map(String s) throws Exception {

        List<String> parts = new ArrayList<>(Arrays.asList(s.split(",")));

        parts.set(1, embeddings.get(parts.get(1)).toString()); // protocol_type
        parts.set(3, embeddings.get(parts.get(3)).toString()); // flag

        Integer lastIndex = parts.size() -1 ;
        String type = parts.get(lastIndex);
        parts.set(lastIndex, type == "normal." ? String.valueOf(0) : String.valueOf(1));

        parts.remove(2);


        return null;
    }
}
