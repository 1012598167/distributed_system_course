package DSPPCode.storm.window_join;

import org.apache.storm.bolt.JoinBolt;

import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import java.util.concurrent.TimeUnit;

public class StormJoinBoltImpl extends StormJoinBolt{
    @Override
    public void setJoinBolt() {

    }

    @Override
    public JoinBolt getJoinBolt() {
//        JoinBolt jbolt =  new JoinBolt("spout1", "key1")                   // from        spout1
//                .join     ("spout2", "userId",  "spout1")      // inner join  spout2  on spout2.userId = spout1.key1
//                .join     ("spout3", "key3",    "spout2")      // inner join  spout3  on spout3.key3   = spout2.userId
//                .leftJoin ("spout4", "key4",    "spout3")      // left join   spout4  on spout4.key4   = spout3.key3
//                .select  ("userId, key4, key2, spout3:key3")   // chose output fields
//                .withTumblingWindow( new Duration(10, TimeUnit.MINUTES) ) ;
        return  new JoinBolt("genderSpout", "id")                   // from        spout1
                .join     ("ageSpout", "id",  "genderSpout")      // inner join  spout2  on spout2.userId = spout1.key1
                .select  ("ageSpout:id,gender,age")   // chose output fields
                .withTumblingWindow( new Duration(2000, TimeUnit.MILLISECONDS)) ;
    }
}
