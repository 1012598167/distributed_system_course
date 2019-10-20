package DSPPCode.hadoop.multi_input_join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PersonMapperImpl extends PersonMapper {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String oneline=value.toString();
//        System.out.println("person"+oneline);
        String[] splitAddress=oneline.split("\\s+");
//        System.out.println(splitAddress[1]+'\t'+splitAddress[2]);
        Text left=new Text();
        Text right=new Text();
        //Text person=Integer.toString(2);
        Text person=new Text("1");
        left.set(splitAddress[1]+'\t'+splitAddress[2]);
        Text zuo=new Text(splitAddress[0]);
        right.set(zuo);
        System.out.println("person_key");
        System.out.println(person);
        System.out.println("pair");

        TextPair pair = new TextPair(left, right);
        System.out.println(pair);
        context.write(person,pair);
    }
}
