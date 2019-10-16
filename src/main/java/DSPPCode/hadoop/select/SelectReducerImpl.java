package DSPPCode.hadoop.select;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class SelectReducerImpl extends SelectReducer{

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> aa=values.iterator();
        System.out.println(key);
        context.write(key,aa.next());
    }
}