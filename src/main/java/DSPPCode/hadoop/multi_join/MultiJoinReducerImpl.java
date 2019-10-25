package DSPPCode.hadoop.multi_join;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class MultiJoinReducerImpl extends MultiJoinReducer {
    private HashMap<String,String> hashMap=new HashMap<>();
    private int a=0;
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String recentkey=key.toString();
        //context.write(new Text("companyname"),new Text("addressname"));
        System.out.println("recentkey");
        System.out.println(recentkey);
//        System.out.println("thevalue");
//        System.out.println(thevalue);
        if(a==0){
            context.write(new Text("companyname"),new Text("addressname"));
            a=1;
        }
//        Text thevalue=new Text();
        for (Text value:values){
            String thevalue=value.toString();
            String[] splitAddress=thevalue.split("\\s+");
            System.out.println("splitAddress[0]");
            System.out.println(splitAddress[0]);
            System.out.println("splitAddress[1]");
            System.out.println(splitAddress[1]);

            if(recentkey.equals("1"))
            {
                hashMap.put(splitAddress[0],splitAddress[1]);
            }
            else
            {
                String x=hashMap.get(splitAddress[0]);
                if(x!=null){
                    context.write(new Text(splitAddress[1]),new Text(x));
                }
            }
//            thevalue=the_next;
//            System.out.println("thevalue");
//            System.out.println(thevalue);
        }


//        context.write(key,thevalue);
    }
}
