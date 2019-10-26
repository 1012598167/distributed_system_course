package DSPPCode.spark.broadcast_join;

import java.util.ArrayList;
import java.util.Iterator;

public class BroadcastJoinMapperImpl extends BroadcastJoinMapper {
    @Override
    public Iterator<String> call(String order) {
        System.out.println(this.persons.getValue());
        String[] splitAddress=order.split(",");
        Long key=new Long(splitAddress[2]);
        String NUM=splitAddress[1];
        if (this.persons.getValue().get(key)!=null){
            System.out.println(this.persons.getValue().get(key));
            System.out.println(this.persons.getValue().get(key)+","+NUM);
            //return (this.persons.getValue()+","+NUM);
            String values=this.persons.getValue().get(key)+","+NUM;
            ArrayList list = new ArrayList();
            list.add(values);
            Iterator iter=list.iterator();
            return iter ;
        }
        ArrayList list = new ArrayList();
        Iterator iter=list.iterator();
        return iter;
    }

}
