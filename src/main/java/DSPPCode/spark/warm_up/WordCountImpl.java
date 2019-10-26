package DSPPCode.spark.warm_up;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Java 答案示例
 */
public class WordCountImpl extends WordCount {

    public JavaPairRDD<String, Integer> wordcount(JavaRDD<String> lines) {

        System.out.println(lines.collect().toString());
        return lines
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator())//把其中一个元素按这个规则（生成一列中每个单词的迭代器）映射
                .mapToPair((String word) -> new Tuple2<>(word, 1))//把映射完的结果组成Tuple
                .reduceByKey((Integer integer, Integer integer2) -> integer + integer2);//reduce过程中把能加的统统加起来
    }

}
