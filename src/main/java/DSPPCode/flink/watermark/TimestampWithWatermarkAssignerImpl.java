package DSPPCode.flink.watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TimestampWithWatermarkAssignerImpl extends TimestampWithWatermarkAssigner {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return  new Watermark(maxTimestamp- MAX_OUT_OF_ORDER);
    }

    @Override
    public long extractTimestamp(Tuple2<Long, Integer> tuple, long unused) {
        maxTimestamp =Math.max(maxTimestamp, tuple.f0);
        return tuple.f0;
    }
}
