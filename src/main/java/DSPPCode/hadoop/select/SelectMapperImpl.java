package DSPPCode.hadoop.select;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import java.util.ArrayList;
public class SelectMapperImpl extends SelectMapper{
    //private List<String> a1=new ArrayList<String>();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //final NullWritable nu= Null;

//        StringTokenizer itr=new StringTokenizer(
//                value.toString()
//        );

        //StringTokenizer keyiter=new StringTokenizer(
        //        key.toString()
        //);
        Text word=new Text();
        //int b=Integer.parseInt((String)os);
        //private static IntWritable thekey=new IntWritable(key);
        String astring= "";

//        List<String> listWithoutDup = new ArrayList<String>(new HashSet<String>(list));
//        Collections.sort(listWithoutDup);
//        for(String str:listWithoutDup){
//            System.out.println(str);
//        }


        //String keystr=keyiter.nextToken();
        //String nexttoken=itr.nextToken();
        String oneline=value.toString();
        String[] splitAddress=oneline.split("\\s+");
        System.out.println(splitAddress[0]+"\t"+splitAddress[1]+splitAddress[2]);

        //String[] datas = value.toString().split(" ");
        word.set(oneline);
        if(splitAddress[2].equals("shanghai"))
        {
        //if (!(a1.contains(splitAddress[2]))){
            System.out.println("写入");
            System.out.println(word);
            context.write(word,NullWritable.get());}
        //a1.add(splitAddress[2]);



    }
}