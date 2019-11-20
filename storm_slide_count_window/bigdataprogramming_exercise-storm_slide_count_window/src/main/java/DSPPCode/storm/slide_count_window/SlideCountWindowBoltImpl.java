package DSPPCode.storm.slide_count_window;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SlideCountWindowBoltImpl extends SlideCountWindowBolt {
    Map<String, String> counts = new HashMap<String, String>();
    /**
     * TODO: 实现此方法每次接收一个Tuple e.g. (a 1)将此tuple放入相应得窗口
     *       同一个key的Tuple每出现两次，对此key最近出现的三个元素进行一次计算 这里为append计算即
     *       (a 1) + (a 2) + (a 3) = (a 123)
     *     注意:emit操作使用outputFormat简化操作
     **/
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//        int n = tuple.size();
        List<Object> k = tuple.getValues();
        String key = k.get(0).toString();
        String value = k.get(1).toString();
        // 获取该单词对应的累计字符串
        String count = counts.get(key);
        if (count == null) {
            count = "";
        }
        // 更新累计字符串
        count = count + value;
        // 将单词和对应的累计字符串加入map中
        counts.put(key, count);
        // 如果累计字符串长度为偶数
        int length = count.length();
        if (length%2 == 0){
            int Num = length / 2;
            String windowNum = String.valueOf(Num);
            if(length < 3){
                String result = outputFormat(key, count, windowNum);
                basicOutputCollector.emit(new Values(result));
            }
            else {
                String str = count.substring(length-3, length);
                String result = outputFormat(key, str, windowNum);
                basicOutputCollector.emit(new Values(result));
            }
        }
    }
}
