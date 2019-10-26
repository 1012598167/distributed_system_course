package DSPPCode.spark.pi;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class PiSimulatorImpl extends PiSimulator {
    @Override
    public Integer call(Object unused) {
//        int n = 100000;
//        List<Object> l = new ArrayList<>(n);
//        for (int i = 0; i < n; i++) {
//            l.add(i);
//        }
//
//        // 执行
//        JavaSparkContext sc = new JavaSparkContext("local", "Pi2");
//
//        JavaRDD<Object> parallelInput = sc.parallelize(l);
//
//        int count = parallelInput
//                .map(new PiSimulatorImpl())
//                .reduce((Integer i1, Integer i2) -> (i1 + i2));
//
//        double pi = 4.0 * count / n;
//        sc.close();

//    double xf = 0.0d;
//    double yf = 0.0d;
//    int total = 0;
//    for(int i = 0;i<100000;i++){
//      xf = Math.random();
//      yf = Math.random();
//      if(Math.sqrt(xf*xf+yf*yf) < 1)
//        total++;
//    }
//    System.out.println(4*(total/1000000.0));

        double x=Math.random();
        double y=Math.random();
        if (x*x+y*y<1){
            return 1;
        }
        else {
            return 0;
        }
    }
}
