package DSPPCode.hadoop.multi_input_join;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class OrderMapperImpl extends OrderMapper{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String oneline=value.toString();
        //System.out.println("order"+oneline);
        String[] splitAddress=oneline.split("\\s+");
        //System.out.println(splitAddress[1]+'\t'+splitAddress[2]);
        Text left=new Text();
        Text right=new Text();
        Text person=new Text("2");
        right.set(person);
        left.set(splitAddress[1]);
        Text zuo=new Text(splitAddress[2]);
        TextPair pair = new TextPair(left, zuo);
        System.out.println("order_key");
        System.out.println(person);
        System.out.println("pair");
        System.out.println(pair);
        context.write(person,pair);
    }
}
