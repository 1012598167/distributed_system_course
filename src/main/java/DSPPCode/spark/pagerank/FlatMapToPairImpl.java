package DSPPCode.spark.pagerank;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatMapToPairImpl extends FlatMapToPair {
    @Override
    public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> outsideWeight) throws Exception {
        System.out.println(outsideWeight._2);
        Double disnow=outsideWeight._2;
        Tuple2<String, Double> atuple=new Tuple2<String, Double>(outsideWeight._1.toString(),outsideWeight._2);
        List<Tuple2<String, Double>> list=new ArrayList<Tuple2<String, Double>>();
        Double length=(double)((outsideWeight._1).spliterator().getExactSizeIfKnown());
        for (String thestring:outsideWeight._1){
            System.out.println(thestring);
            list.add(new Tuple2<String,Double>(thestring,disnow/length));
        }
        return list.iterator();
    }
}
