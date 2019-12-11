package DSPPCode.flink.k_means;

import DSPPCode.flink.k_means.util.Centroid;
import DSPPCode.flink.k_means.util.Point;
import org.apache.flink.api.java.DataSet;

public class IterationStepImpl extends IterationStep {
    @Override
    public DataSet<Centroid> runStep(DataSet<Point> points, DataSet<Centroid> centroid) {

        return null;
    }
}
