package DSPPCode.spark.k_means;

import org.apache.spark.mllib.linalg.Vector;

import java.util.List;

public class KMeansImpl extends KMeans {
    @Override
    int closestPoint(Vector point, List<Vector> centers) {
        double sp=1000000000;
        int output=0;
        int i=0;
//        System.out.println("point:");
//        System.out.println(point.toArray()[0]);
//        System.out.println(point.toArray()[1]);
        for(Vector center:centers){
            if(squaredDistance(point,center)<sp){
                sp=squaredDistance(point,center);
                output=i;
            }
            i+=1;
//            System.out.println("center");
//            System.out.println(center);
        }
        return output;
    }

    @Override
    double squaredDistance(Vector point, Vector index) {
        System.out.println(point);
        System.out.println(index);
        return Math.pow(point.toArray()[0]-index.toArray()[0],2)+Math.pow(point.toArray()[1]-index.toArray()[1],2);
    }
}
