package DSPPCode.storm.top_n;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.storm.tuple.Values;
public class TopicCountBoltImpl extends TopicCountBolt {
    @Override
    void processTweet(String tweet) {
        System.out.println(tweet);
        String pattern = "#(\\w+)\\W?";
        Pattern poundkey = Pattern.compile(pattern);
        Matcher deal = poundkey.matcher(tweet);
//        System.out.println(poundkey.split(tweet)[0]);
//        System.out.println(deal.matches());
//        deal.find();
//        System.out.println(deal.group(1));
//        System.out.println(deal.group(1));
        while (deal.find()) {
            String words = deal.group(1);
            System.out.println(words);
            if (counter.containsKey(words)) {
                counter.put(words, counter.get(words) + 1);
            } else {
                counter.put(words, 1);
            }
        }

    }

    @Override
    void reportTopNToPrinter() {
        String result = "";
        Map<String, Integer> iter =  counter;
        Comparator<Map.Entry<String, Integer>> valueComparator = new Comparator<Map.Entry<String,Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return o2.getValue()-o1.getValue();
            }
        };
        // map转换成list进行排序
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String,Integer>>(iter.entrySet());
// 排序
        Collections.sort(list,valueComparator);
        int i=topN-1;
        for (Map.Entry<String, Integer> entry : list) {
            result += entry.getKey() + "," + entry.getValue() + "\n";
            i--;
            if(i<0){break;}
        }
//        while (iter.hasNext()) {
//            Map.Entry<String, Integer> entry = iter.next();
//            result += entry.getKey() + "," + entry.getValue() + "\n";
//        }
        result.trim();
        collector.emit(new Values(result));
    }
}
