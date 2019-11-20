package DSPPCode.storm.window_join;

import DSPPCode.storm.window_join.util.FileProcess;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;

public class PrinterBoltImpl extends PrinterBolt {
    public PrinterBoltImpl(String outputFile) {
        super(outputFile);
    }

    @Override
    public String parseTuple(Tuple tuple) {
        return "[" + tuple.getInteger(0) + ", " + tuple.getString(1) + ", "
                + tuple.getInteger(2) + "]\n";
    }

    @Override
    public void saveResult(String outputFile, String result) {
        BufferedWriter bw = FileProcess.getWriter(outputFile);
        FileProcess.write(result, bw);
        FileProcess.close(bw);
    }
}
