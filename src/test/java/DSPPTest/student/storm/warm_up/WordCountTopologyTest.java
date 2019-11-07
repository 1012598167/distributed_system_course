package DSPPTest.student.storm.warm_up;

import DSPPCode.storm.warm_up.CountBoltImpl;
import DSPPCode.storm.warm_up.PrinterBolt;
import DSPPCode.storm.warm_up.WordSpout;
import DSPPTest.student.TestTemplate;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.daemon.supervisor.ReadClusterState;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Test;

import java.io.File;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTopologyTest extends TestTemplate {

    @Test
    public void test() throws Exception {
        // 设置路径
        String inputFile = root + "/storm/warm_up/input";
        String outputFolder = outputRoot + "/storm/warm_up";
        String outputFile = outputFolder + "/warm_up";
        String answerFile = root + "/storm/warm_up/answer";

        // 删除旧输出
        deleteFolder(outputFolder);

        // 执行
        String topologyName = "wc";
        String spoutid = "word_spout";
        String countid = "counter";
        String printerid = "printer";

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutid, new WordSpout(inputFile));
        builder.setBolt(countid, new CountBoltImpl()).fieldsGrouping(spoutid, new Fields("word"));
        builder.setBolt(printerid, new PrinterBolt(outputFile, 1)).globalGrouping(countid);

        Logger.getLogger(ReadClusterState.class).setLevel(Level.FATAL);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, new Config(), builder.createTopology());
        long t = System.currentTimeMillis();
        while (System.currentTimeMillis() - t < 60000) {
            File file = new File(outputFile);
            if (file.exists()) {
                break;
            }
            Thread.sleep(1000);
        }
        cluster.killTopology(topologyName);
        cluster.shutdown();

        //检验结果
        verifyKV(readFile2String(outputFile), readFile2String(answerFile));

        System.out.println("恭喜通过~");
    }

}
