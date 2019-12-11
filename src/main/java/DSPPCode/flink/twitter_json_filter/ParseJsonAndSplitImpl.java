package DSPPCode.flink.twitter_json_filter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class ParseJsonAndSplitImpl extends ParseJsonAndSplit {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
        value=value.toLowerCase();
        jsonParser=new ObjectMapper();
        JsonNode thejson = jsonParser.readTree(value);
        String text = thejson.get("text").asText();
        String lang=thejson.get("user").get("lang").asText();
        Map<String,Integer> themap=new HashMap<>();
        if (lang.equals("en")){
            for (String token : text.split(" ")) {
//                if (themap.containsKey(token)) {
//                    themap.put(token,themap.get(token)+1);
//                }
//                else
//                    themap.put(token,1);
                if (token.length() > 0) {
                    collector.collect(new  Tuple2<>(token, 1));
                }
            }
//            for(Map.Entry<String, Integer> entry : themap.entrySet())
//                collector.collect(new Tuple2<>(entry.getKey(),entry.getValue()));

        }

        //遍历方法2
//        StringTokenizer itr=new StringTokenizer(text," ");
//        while (itr.hasMoreTokens()){
//            String token=itr.nextToken();
//            collector.collect(new  Tuple2<>(token, 1));
//        }





        //下面没用
//        Collector<Tuple2<String, Integer>> result0 = value
//                .map(new MapFunction<String, Collector<Tuple2<String, Integer>>>() {
//                         @Override
//                         public Collector<Tuple2<String, Integer>> map(String value) throws Exception {
//                             Collector<Tuple2<String, Integer>> out = null;
//                             for (String token : value.split("\\W+")) {
//                                 if (token.length() > 0) {
//                                     out.collect(new Tuple2<>(token, 1));
//                                 }
//                             }
//                             return out;
//                         }
//                     });
    }
}
