package DSPPCode.storm.slide_count_window;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlideCountWindowBoltImpl extends SlideCountWindowBolt {

    class Window {
        String value;
        int windowNum;//移动窗口是该key第几个
        int sign;//信号量是1的时候要输出

        public Window(String value_, int windowNum_, int sign_) {
            this.value = value_;
            this.windowNum = windowNum_;
            this.sign = sign_;
        }

        public int getsign() {
            if (sign==1)
                return sign--;
            else
                return sign++;
        }


        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getWindowNum() {
            return windowNum++;
        }


    }
    Map<String, Window> map = new HashMap<>();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String key = tuple.getString(0);
        String value = tuple.getString(1);

        if (!map.containsKey(key)) {
            // map里面没有这种key:新建一个(value,1,1)
            Window wind = new Window(value, 1, 1);
            map.put(key, wind);

        }
        else {
            if (map.get(key).getsign() == 0)
                map.get(key).setValue(map.get(key).getValue().substring(map.get(key).getValue().length()-1)+value);//12取2加3
            else {
                map.get(key).setValue(map.get(key).getValue()+value);
                basicOutputCollector.emit(new Values(outputFormat(key, map.get(key).getValue(), Integer.toString(map.get(key).getWindowNum())))
                );
            }
        }

    }
}
