package DSPPCode.hadoop.multi_join;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class MultiJoinMapperImpl extends MultiJoinMapper {
    private Text one=new Text("1");
    private Text two=new Text("2");
    private Text SHIFT=new Text();
//    private int a=0;
//    private HashMap<String,String> hashMap=new HashMap<>();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String thevalue=value.toString();
        String[] splitAddress=thevalue.split("\\s+");
        String left=splitAddress[0];
        String right=splitAddress[1];
//        if(a==0){
//            context.write(new Text("companyname"),new Text("addressname"));
//            a=1;
//        }
        if (left.equals("addressid")){
            SHIFT=one;
        }else if (left.equals("companyname")){
            SHIFT=two;
        }else
        {
            if (SHIFT==one){
                context.write(SHIFT,new Text(left+"\t"+right));
//                hashMap.put(left,right);
            }
            else {
//                String x=hashMap.get(right);
//                if (x!=null){
//                    context.write(new Text(left),new Text(x));
//                }
                context.write(SHIFT,new Text(right+"\t"+left));
            }
        }
//        System.out.println("key");
//        System.out.println(key);
//        System.out.println("thevalue");
//        System.out.println(thevalue);
//        context.write(new Text(thevalue),new Text(thevalue));

    }
}
