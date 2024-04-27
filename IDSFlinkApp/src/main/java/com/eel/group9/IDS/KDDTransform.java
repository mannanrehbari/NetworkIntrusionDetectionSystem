package com.eel.group9.IDS;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class KDDTransform implements MapFunction<String, String> {

    Map<String, Integer> embeddings;
    Map<Integer, Integer> scalingMap;

    long [] maxAtIndex = new long[33];
    List<Integer> stdIndexList = new ArrayList<>(Arrays.asList(0, 1, 3, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 22, 23, 31, 32));

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

        // maxAtIndex should have all 1 values by default
        for (int i = 0; i < maxAtIndex.length; i++) {
            maxAtIndex[i] = 1;
        }
    }

    @Override
    public String map(String s) throws Exception {

        // Indexes of numeric columns
        //0,icmp,ecr_i,SF,520,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,511,511,0.0,0.0,0.0,0.0,1.0,0.0,0.0,255,255,1.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,smurf.
        List<String> parts = new ArrayList<>(Arrays.asList(s.split(",")));
        int rawLength = parts.size();

        parts.set(1, embeddings.get(parts.get(1)).toString()); // protocol_type
        parts.set(3, embeddings.get(parts.get(3)).toString()); // flag

        Integer lastIndex = parts.size() -1 ;
        String type = parts.get(lastIndex);
        parts.set(lastIndex, type == "normal." ? String.valueOf(0) : String.valueOf(1));

        stdIndexList.forEach(index -> {
            maxAtIndex[index] = Math.max(maxAtIndex[index], Integer.parseInt(parts.get(index)));
            parts.set(index, String.valueOf(Integer.parseInt(parts.get(index)) / maxAtIndex[index]));
        });

        parts.remove(2);
        int transformedLength = parts.size();
        String transformedStr = String.join(",", parts);
        return transformedStr;
    }
}
