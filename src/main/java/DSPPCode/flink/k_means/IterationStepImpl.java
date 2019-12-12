package DSPPCode.flink.k_means;

import DSPPCode.flink.k_means.util.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class IterationStepImpl extends IterationStep {
    @Override
    public DataSet<Centroid> runStep(DataSet<Point> points, DataSet<Centroid> centroid) {
        SelectNearestCenter SNC = new SelectNearestCenter();
        CentroidAccumulator CAc =new CentroidAccumulator();
        CentroidAverager CAv =new CentroidAverager();
        CountAppender CAp =new CountAppender();
//        return points.map(x->SNC.map(x))
//                .map(x-> CAp.map(x))
//                .groupBy(0).reduce((x1, x2)->CAc.reduce(x1,x2))
//                .map(x->CAv.map(x));

        return points.map(new SelectNearestCenter()).withBroadcastSet(centroid,"centroids")
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                .map(new CentroidAverager());

//        return points.map(x->SNC.map(x))
//                .map(x-> CAp.map(x))
//                .groupBy(0).reduce((x1, x2)->CAc.reduce(x1,x2))
//                .map(x->CAv.map(x)).returns(TypeInformation.of(new TypeHint<Centroid>() {}));
    }
}
