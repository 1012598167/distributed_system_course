package DSPPTest.student.storm.window_join;

import DSPPCode.storm.window_join.PrinterBolt;
import DSPPCode.storm.window_join.PrinterBoltImpl;
import DSPPCode.storm.window_join.StormJoinBolt;
import DSPPCode.storm.window_join.StormJoinBoltImpl;
import DSPPTest.student.TestTemplate;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.File;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;
import static DSPPTest.util.Verifier.verifyList;

/**
 * @author chenqh
 * @version 1.0.0
 * @date 2019-11-04
 */
public class WindowJoinTest extends TestTemplate {
    @Test
    public void test() throws Exception {

        String outputFolder = outputRoot + "/storm/window_join";
        String outputFile = outputFolder + File.separator + "storm/window_join";
        String answerFile = root + "/storm/window_join/answer";

        deleteFolder(outputFolder);
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("ageSpout", ageSpout, 1);
        builder.setSpout("genderSpout", genderSpout, 1);
        // inner join of 'age' and 'gender' records on 'id' field
        StormJoinBolt JoinBolt = new StormJoinBoltImpl();

        org.apache.storm.bolt.JoinBolt joiner = JoinBolt.getJoinBolt();

        builder.setBolt("joiner", joiner, 1)
                .fieldsGrouping("genderSpout", new Fields("id"))
                .fieldsGrouping("ageSpout", new Fields("id"));
        PrinterBolt printerBolt = new PrinterBoltImpl(outputFile);
        builder.setBolt("printer", printerBolt, 1).shuffleGrouping("joiner");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-example", conf, builder.createTopology());
        generateGenderData(genderSpout);

        generateAgeData(ageSpout);

        Thread.sleep(8000);
        cluster.killTopology("join-example");
        cluster.shutdown();
        verifyList(readFile2String(outputFile), readFile2String(answerFile));
        System.out.println("恭喜通过~");
    }
    private static void generateAgeData(FeederSpout ageSpout) {
        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }
    }

    private static void generateGenderData(FeederSpout genderSpout) {
        for (int i = 0; i < 10; i++) {
            String gender;
            if (i % 2 == 0) {
                gender = "male";
            } else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }
    }
}
