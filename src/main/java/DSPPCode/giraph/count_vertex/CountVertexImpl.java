package DSPPCode.giraph.count_vertex;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Iterator;

public class CountVertexImpl extends CountVertex {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> iterable) throws IOException {
        if (getSuperstep()==0)
        {
//            if (bool){
            count.set(count.get()+1);
            lastvertex.add(vertex.getId());
            Iterator<IntWritable> collt=lastvertex.iterator();
            sendMessageToMultipleEdges(collt,count);
//            }
//            else{
//                vertex.setValue(count);
//                vertex.voteToHalt();
//            }


        }
        else
        {
            IntWritable a=new IntWritable(0);
            for (IntWritable message:iterable){
                if(a.get()<message.get()){
                    a=message;
                }
            }
            vertex.setValue(a);
            vertex.voteToHalt();
        }

//        for (IntWritable iter:iterable){
//            count+=1;
//        }
//        sendMessage();
//        vertex.voteToHalt();//休眠
    }
}
