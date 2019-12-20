package DSPPCode.giraph.page_rank;

import it.unimi.dsi.fastutil.ints.IntIterable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Iterator;

public class PageRankImpl extends PageRank {
    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> iterable) throws IOException {
        LongWritable id = vertex.getId();
        DoubleWritable value=vertex.getValue();
        if(getSuperstep()==0){
            System.out.println("----------------Superstep"+getSuperstep()+"--------------------------");
            System.out.println("----------------id"+id+"--------------------------");
            for(Edge<LongWritable, FloatWritable> edge:vertex.getEdges()){
                System.out.println("-----edgeiter"+"-----");
                LongWritable theid = edge.getTargetVertexId();
                System.out.println("-----theid"+theid+"-----");
                FloatWritable pr = edge.getValue();
                vertex.setEdgeValue(theid,pr);
                System.out.println("vertex.getValue()"+vertex.getValue());
                System.out.println("vertex.getValue()"+vertex.getNumEdges());
                sendMessage(theid,new DoubleWritable(vertex.getValue().get()/vertex.getNumEdges()));
            }
            sendMessage(vertex.getId(),new DoubleWritable(0));
//            vertex.setValue();
            vertex.voteToHalt();
        }
        else if (getSuperstep()<MAX_SUPERSTEP){
//            Iterator<DoubleWritable> iter = iterable.iterator();
            System.out.println("----------------Superstep"+getSuperstep()+"--------------------------");
            System.out.println("----------------id"+id+"--------------------------");
            sendMessage(vertex.getId(),new DoubleWritable(0));
            DoubleWritable thefinal=new DoubleWritable(0);
//            for(Edge<LongWritable, FloatWritable> edge:vertex.getEdges()){
//                LongWritable theid = edge.getTargetVertexId();
//                DoubleWritable pr = iter.next();
//                DoubleWritable l = iter.next();
//                sendMessage(theid,new DoubleWritable(vertex.getNumEdges()));
//
//                thefinal=new DoubleWritable(thefinal.get()+pr.get()/l.get());
//            }
            for (DoubleWritable messages:iterable){
                thefinal=new DoubleWritable(thefinal.get()+messages.get());
            }
            System.out.println("-----thefinalbefore"+thefinal+"----");
            thefinal=new DoubleWritable(thefinal.get()*D+(1-D)/getTotalNumVertices());/////////////////3
            System.out.println("-----thefinalafter"+thefinal+"----");
            vertex.setValue(thefinal);
            for(Edge<LongWritable, FloatWritable> edge:vertex.getEdges()){
                LongWritable theid = edge.getTargetVertexId();
//                sendMessage(theid,new DoubleWritable(vertex.getValue().get()/vertex.getNumEdges()));
                sendMessage(theid,new DoubleWritable(thefinal.get()/vertex.getNumEdges()));
            }
            vertex.voteToHalt();
        }
        else {
            vertex.voteToHalt();
        }
    }
}
