package DSPPCode.flink.stream_warm_up;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.python.indexer.ast.NLambda;

import java.util.Arrays;

/**
 * Java答案示例
 */
public class WordCountImpl extends WordCount {

    @Override
    public DataStream<Tuple2<String, Integer>> wordCount(DataStream<String> text) {
        DataStream<String> dataStream2 = //...
                text.map(new MapFunction<String ,String>() {
                    @Override
                    public String map(String value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                });
//        text.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out)
//                    throws Exception {
//                for(String word: value.split(" ")){
//                    out.collect(word);
//                }
//            }
//        });


//        DataStream<Collector<Tuple2<String, Integer>>> result0 = text
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


        DataStream<Tuple2<String, Integer>> result11 = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String token : value.split("\\W+")) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                });
//////        DataStream<Tuple2<String, Integer>> result1 = text
//////                .flatMap((FlatMapFunction<String,Tuple2<String, Integer>>) (value, out) -> {
//////                    for (String token : value.split("\\W+")) {
//////                        if (token.length() > 0) {
//////                            out.collect(new Tuple2<>(token, 1));
//////                        }
//////                    }
//////                });
//        int[] array = {7, -2, 3, 5, -9, 3, -5, -1, 6, 8, 20};
//        Arrays.stream(array)
//                .filter(a -> a >= 0)    //过滤
//                .sorted()               //排序
//                .forEach(System.out::println);

//        DataStream<Tuple2<String, Integer>> maptry = text.map(x -> new Tuple2<>(x, 1));
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> result2 = result11.keyBy(0).timeWindow(Time.seconds(1), Time.seconds(1));
        DataStream<Tuple2<String, Integer>> result3 = result2.sum(1);
        return result3;
//        return text
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//                        for (String token : value.split("\\W+")) {
//                            if (token.length() > 0) {
//                                out.collect(new Tuple2<>(token, 1));
//                            }
//                        }
//                    }
//                })
//                .keyBy(0)
////                .timeWindow(Time.seconds(1), Time.seconds(1))
//                .sum(1);
    }

}
