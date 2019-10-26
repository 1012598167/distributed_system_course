package DSPPCode.hadoop.ssp;

import com.sun.org.apache.xerces.internal.util.TeeXMLDocumentFilterImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import py4j.StringUtil;
import scala.reflect.internal.Names;

import java.io.IOException;

public class SimpleShortestPathsMapperImpl extends SimpleShortestPathsMapper {
    /* {B, {10 (C,1) (D,2)}, {8}, {12}}   =>  B, 8 (C,1) (D,2)*/
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String thekey=key.toString();
        String thevalue=value.toString();
        System.out.println("key"+thekey);
        System.out.println("value"+thevalue);
        Node node=new Node();
        String[] splitAddress=thevalue.split("\\t+");
        if (!(StringUtils.isNumeric(splitAddress[0]) || splitAddress[0].equals("inf"))){//第一轮
            if(thekey.equals("A")){
                node.FormatNode("0"+"\t"+thevalue);
                context.write(key,new Text("0"+"\t"+thevalue));
                for(int i=0;i<node.getNodeNum();i++){
                    context.write(new Text(node.getNodeKey(i)),new Text(node.getNodeValue(i)));
                }

            }
            else {
                node.FormatNode("inf"+"\t"+thevalue);
                context.write(key,new Text("inf"+"\t"+thevalue));
            }
        }
        else {
            node.FormatNode(thevalue);
            context.write(key,value);
            if (!(node.getDistance().equals("inf"))){
                for(int i=0;i<node.getNodeNum();i++){
                    System.out.println(node.getNodeKey(i)+String.valueOf(Integer.parseInt(node.getNodeValue(i))+Integer.parseInt(node.getDistance())));
                    context.write(new Text(node.getNodeKey(i)),new Text(String.valueOf(Integer.parseInt(node.getNodeValue(i))+Integer.parseInt(node.getDistance()))));
                }
            }

        }
//        node.setDistance(String.valueOf(1));
//        context.write(key,value);


//        int conuter = context.getConfiguration().getInt("run.counter", 1);
//
//        Node node = new Node();
//        String distance = null;
//        String str = null;
//
//        // 第一次计算，填写默认距离 A:0 其他:inf
//        if (conuter == 1) {
//            if (key.toString().equals("A") || key.toString().equals("1")) {
//                distance = "0";
//            } else {
//                distance = "inf";
//            }
//            str = distance + "\t" + value.toString();
//        } else {
//            str = value.toString();
//        }
//
//        context.write(key, new Text(str));
//
//        node.FormatNode(str);
//
//        // 没走到此节点 退出
//        if (node.getDistance().equals("inf"))
//            return;
//
//        // 重新计算源点A到各点的距离
//        for (int i = 0; i < node.getNodeNum(); i++) {
//            String k = node.getNodeKey(i);
//            String v = new String(
//                    Integer.parseInt(node.getNodeValue(i)) + Integer.parseInt(node.getDistance()) + "");
//            context.write(new Text(k), new Text(v));
//        }
    }
}
