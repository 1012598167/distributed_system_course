package DSPPCode.hadoop.multi_input_join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class ReduceJoinReducerImpl extends ReduceJoinReducer {
    private HashMap<String,String> hashMap=new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
        String recentkey=key.toString();
        System.out.println("############################"+recentkey);
        //Iterator<TextPair> iter=values.iterator();
        //TextPair the_next=iter.next();

        for (TextPair the_next : values) {
            String left =the_next.getData().toString();
            String right = the_next.getFlag().toString();
            System.out.println("//////");
            System.out.println(recentkey);
            System.out.println(left);
            System.out.println(right);
            System.out.println("//////");

            if(recentkey.equals("1")){
                hashMap.put(right,left);
            }
            else
            {
                Text RESULT=new Text();
                String x = hashMap.get(right);
                if(x!=null){
                    String y=x+"\t"+left+'\t';
                    RESULT.set(y);
                    System.out.println("??????");
                    System.out.println(y);
                    System.out.println("??????");
                    context.write(RESULT, NullWritable.get());
                }

            }

        }


        //text是每返回一个结果context就测试一次
        //1 Adams\tJohn 2
        //2 Bush\tGeorge 2
        //3 Carter\tThomas 2 先收到person的结果
        //65 34764 1
        //
        //context：
        //Adams	John	24562
        //Adams	John	22456
        //Carter	Thomas	44678
        //Carter	Thomas	77895
    }
}
