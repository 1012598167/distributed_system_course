package DSPPCode.flink.capacity_monitor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CapacityMonitorFunctionImpl extends CapacityMonitorFunction {
    @Override
    public void flatMap(Tuple2<String, Integer> tuple, Collector<Tuple2<String, Boolean>> collector) throws Exception {
        String thestring=tuple.f0;
        Integer theinteger=tuple.f1;

        if (themap.containsKey(thestring)){
            Integer before=themap.get(thestring);
            Integer after =themap.get(thestring)+theinteger;
            themap.put(thestring,after);
            if(before<=THRESHOLD && after>THRESHOLD){
                collector.collect(new Tuple2<>(thestring,true));
            }
            else if(before>THRESHOLD && after<=THRESHOLD){
                collector.collect(new Tuple2<>(thestring,false));
            }
        }
        else{
            themap.put(thestring,theinteger);
        }
    }
}
