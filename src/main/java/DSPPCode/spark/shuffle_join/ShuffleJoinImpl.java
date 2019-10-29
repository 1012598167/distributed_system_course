package DSPPCode.spark.shuffle_join;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ShuffleJoinImpl extends ShuffleJoin {
    public Map<Long,String> themap=null;
    @Override
    public JavaRDD<String> join(JavaPairRDD<Long, String> personRdd, JavaPairRDD<Long, String> orderRdd) {
        JavaPairRDD<Long, Tuple2<String, String>> ardd=personRdd.join(orderRdd);
        JavaPairRDD<Long,String> newardd=ardd.mapValues(v->v._1+","+v._2);
//        System.out.println(ardd.collect());
//        return ardd.map(new Function<Tuple2<Long, Tuple2<String, String>>, String>() {
//            @Override
//            public String call(Tuple2<Long, Tuple2<String, String>> longTuple2Tuple2) throws Exception {
//                return longTuple2Tuple2._2._1+","+longTuple2Tuple2._2._2;
//            }
//        });
        return  newardd.values();
//        //System.out.println(personRdd.collectAsMap().get(1));//1：Adams,John
//        //System.out.println(orderRdd.collect());//1：24562
//        //Map<Long,String> themap=personRdd.collectAsMap();
//        themap=personRdd.collectAsMap();
//        //ForeachfunctionTest a = new ForeachfunctionTest();
////        orderRdd.foreach(new VoidFunction<Tuple2<Long, String>>() {
////            @Override
////            public void call(Tuple2<Long, String> longStringTuple2) throws Exception {
////                Long left=longStringTuple2._1();
////                String right=longStringTuple2._2();
////                System.out.println(themap.get(left)+","+right);
////            }
////
////        });
//        ForeachfunctionTest thetest = new ForeachfunctionTest(themap);
//        orderRdd.foreach(thetest);
//        //System.out.println(orderRdd.collect());
//        System.out.println(1);
//        return thetest.getContext();
    }
}
//class ForeachfunctionTest implements VoidFunction<Tuple2<Long, String>> {
//    Map<Long,String> themap = null;
//    JavaRDD<String> context=null;
//    List<String> list_context= new ArrayList<>();
//    ForeachfunctionTest(Map<Long,String> themaps_) {
//        this.themap = themaps_;
//        this.context=null;
//        this.list_context= new ArrayList<>();
//    }
//    public JavaRDD<String> getContext(){
//        context=new JavaSparkContext().parallelize(list_context);
//        return context;
//    }
//
//    public void call(Tuple2<Long, String> longStringTuple2) throws Exception {
//        Long left=longStringTuple2._1();
//        String right=longStringTuple2._2();
//        //ShuffleJoinImpl a  = new ShuffleJoinImpl();
//        if (!(themap.get(left)==null)){
//            System.out.println(themap.get(left)+","+right);
//            list_context.add(themap.get(left)+","+right);
//        }
//
//    }
//}



//
//class ForeachfunctionTest2 implements VoidFunction<Tuple2<Long, Tuple2<String, String>> {
//    @Override
//    public String call(Tuple2<Long, Tuple2<String, String>> longTuple2Tuple2) throws Exception {
//        return longTuple2Tuple2._2._1+","+longTuple2Tuple2._2._2;
//    }
//}