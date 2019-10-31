package DSPPCode.spark.pagerank;

import scala.Tuple2;

public class CalculateRankImpl extends CalculateRank {
    @Override
    public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> weight) throws Exception {
        System.out.println(weight._1);
        double thesum=0;
        for (Double thedouble:weight._2){
            System.out.println(thedouble);
            thesum+=thedouble;

        }
        return new Tuple2<String, Double>(weight._1,thesum*0.85+0.15);
    }
}
