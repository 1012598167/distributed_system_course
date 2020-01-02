package DSPPCode.giraph.count_vertex;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Iterator;

import static DSPPCode.giraph.count_vertex.CountVertexMasterCompute.COUNT_VERTEX;

public class CountVertexImpl extends CountVertex {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> iterable) throws IOException {

        //法1
//        if (getSuperstep()==0)
//        {
////            if (bool){
//            count.set(count.get()+1);
//            lastvertex.add(vertex.getId());
//            Iterator<IntWritable> collt=lastvertex.iterator();
//            sendMessageToMultipleEdges(collt,count);
////            }
////            else{
////                vertex.setValue(count);
////                vertex.voteToHalt();
////            }
//
//
//        }
//        else
//        {
//            IntWritable a=new IntWritable(0);
//            for (IntWritable message:iterable){
//                if(a.get()<message.get()){
//                    a=message;
//                }
//            }
//            vertex.setValue(a);
//            vertex.voteToHalt();
//        }
//
////        for (IntWritable iter:iterable){
////            count+=1;
////        }
////        sendMessage();
////        vertex.voteToHalt();//休眠
        //法2
//        vertex.setValue(new IntWritable((int) getTotalNumVertices()));
//        vertex.voteToHalt();
        //法3
        if(getSuperstep()==0){
            aggregate(COUNT_VERTEX,vertex.getValue());
        }
        else{
            vertex.setValue(getAggregatedValue(COUNT_VERTEX));//给所有节点发和
            vertex.voteToHalt();
        }

    }
}
