package DSPPCode.hadoop.ssp;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class SimpleShortestPathsReducerImpl extends SimpleShortestPathsReducer {
    /* {B, {10 (C,1) (D,2)}, {8}, {12}}   =>  B, 8 (C,1) (D,2)*/
    private String smalldis="inf";
    private Node nodeold=new Node();
    //int data2[] = null;

    @Override
    public void reduce(Text nodeKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int[] data2 = new int[10];
        Arrays.fill(data2,0);
        int i=0;
        String thekey=nodeKey.toString();
        System.out.println("nodeKey"+thekey);
        Iterator itr=values.iterator();
        Node node=new Node();
        while(itr.hasNext())
        {
            String theiter=itr.next().toString();
//            if (!(StringUtils.isNumeric(theiter))){
            node.FormatNode(theiter);
//            }
            if(node.getNodeNum()!=0){//前面已经运行过!=0
                nodeold.FormatNode(theiter);
                if(node.getDistance().equals("inf")){
                    smalldis="inf";
                }
                else{
                    int a=Integer.parseInt(node.getDistance());
                    smalldis= String.valueOf(a);
                }
            }
            else
            {
                //isChange(node,smalldis,context);
                data2[i++]=Integer.parseInt(node.getDistance());
//                if(smalldis.equals("inf") || Integer.parseInt(node.getDistance())<=Integer.parseInt(smalldis)){
//                    smalldis=node.getDistance();
//                }
            }
        }
        if (i!=0){
            Arrays.sort(data2);
            if(smalldis.equals("inf") || data2[10-i]<=Integer.parseInt(smalldis)){
                smalldis=String.valueOf(data2[10-i]);
            }
        }

        isChange(nodeold,smalldis,context);
        nodeold.setDistance(smalldis);
        context.write(nodeKey,new Text(nodeold.toString()));
//        String min = null;
//        int i = 0;
//        String dis = "inf";
//        Node node = new Node();
//        for (Text t : arg1) {
//            i++;
//            dis = StringUtils.split(t.toString(), '\t')[0];
//
//            // 如果存在inf节点，表示存在没有计算距离的节点。
//            // if(dis.equals("inf"))
//            // arg2.getCounter(eInf.COUNTER).increment(1L);
//
//            // 判断是否存在相邻节点，如果是则需要保留信息，并找到最小距离进行更新。
//            String[] strs = StringUtils.split(t.toString(), '\t');
//            if (strs.length > 1) {
//                node.FormatNode(t.toString());
//            }
//
//            // 第一条数据默认是最小距离
//            if (i == 1) {
//                min = dis;
//            } else {
//                if (dis.equals("inf"))
//                    ;
//                else if (min.equals("inf"))
//                    min = dis;
//                else if (Integer.parseInt(min) > Integer.parseInt(dis)) {
//                    min = dis;
//                }
//            }
//        }
//
//        // 有新的最小值，说明还在进行优化计算，需要继续循环计算
//        if (!min.equals("inf")) {
//            if (node.getDistance().equals("inf"))
//                arg2.getCounter(eInf.COUNTER).increment(1L);
//            else {
//                if (Integer.parseInt(node.getDistance()) > Integer.parseInt(min))
//                    arg2.getCounter(eInf.COUNTER).increment(1L);
//            }
//        }
//
//        node.setDistance(min);
//
//        arg2.write(arg0, new Text(node.toString()));
    }

}
