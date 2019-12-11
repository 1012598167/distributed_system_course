package DSPPCode.flink.twitter_json_filter;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterImpl extends Filter {
    /**
     * 给路径赋值，调用读取停词文件数据方法
     * 在实现类Impl中需要实现此构造方法
     *
     * @param stopWordPath 文件路径
     */
    FilterImpl(String stopWordPath) throws IOException {
        super(stopWordPath);
    }

    @Override
    public void readStopWords(String stopWordPath) throws IOException {
    System.out.println(stopWordPath);
        try {
            File file = new File(stopWordPath);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String strLine = null;
            int lineCount = 1;
            while(null != (strLine = bufferedReader.readLine())){
//                System.out.println("第[" + lineCount + "]行数据:[" + strLine + "]");
                stopWords.add(strLine);
                lineCount++;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean filter(Tuple2<String, Integer> wordcount) throws Exception {
        return wordcount.f1 >= 4 && !(stopWords.contains(wordcount.f0));
    }
}
