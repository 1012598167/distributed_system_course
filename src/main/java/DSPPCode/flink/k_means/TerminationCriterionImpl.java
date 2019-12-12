package DSPPCode.flink.k_means;

import DSPPCode.flink.k_means.util.Centroid;
import DSPPCode.flink.k_means.util.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class TerminationCriterionImpl extends TerminationCriterion {
    @Override
    public FilterOperator<Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>> getTerminatedDataSet(DataSet<Centroid> newCentroids, DataSet<Centroid> oldCentroids) {
        JoinOperator.DefaultJoin<Centroid, Centroid> a1=newCentroids.join(oldCentroids).where("id").equalTo("id");
        MapOperator<Tuple2<Centroid, Centroid>, Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>> a2 = a1.map(new MapFunction<Tuple2<Centroid, Centroid>, Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>>() {
            @Override
            public Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>> map(Tuple2<Centroid, Centroid> value) throws Exception {
                Tuple3<Integer, Double, Double> x1 = new Tuple3<>(value.f0.id, value.f0.x, value.f0.y);
                Tuple3<Integer, Double, Double> x2 = new Tuple3<>(value.f1.id, value.f1.x, value.f1.y);
                return new Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>(x1, x2);
            }
        });
        return a2.filter(new FilterFunction<Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>>() {
            @Override
            public boolean filter(Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>> value) throws Exception {
                return Math.abs(new Point(value.f0.f1,value.f0.f2).euclideanDistance(new Point(value.f1.f1,value.f1.f2)))>EPSILON;
            }
        });
    }
}
