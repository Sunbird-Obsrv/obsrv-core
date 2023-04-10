package org.sunbird.obsrv.core.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

public class TumblingProcessingTimeCountWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;
    private final long size;
    private final long globalOffset;
    private Long staggerOffset = null;
    private final WindowStagger windowStagger;

    private TumblingProcessingTimeCountWindows(long size, long maxCount, long offset, WindowStagger windowStagger) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
        } else {
            this.size = size;
            this.maxCount = maxCount;
            this.globalOffset = offset;
            this.windowStagger = windowStagger;
        }
    }

    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        long now = context.getCurrentProcessingTime();
        if (this.staggerOffset == null) {
            this.staggerOffset = this.windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), this.size);
        }

        long start = TimeWindow.getWindowStartWithOffset(now, (this.globalOffset + this.staggerOffset) % this.size, this.size);
        return Collections.singletonList(new TimeWindow(start, start + this.size));
    }

    public long getSize() {
        return this.size;
    }

    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return CountProcessingTimeTrigger.of(this.maxCount);
    }

    public String toString() {
        return "TumblingProcessingTimeWindows(" + this.size + ")";
    }

    public static TumblingProcessingTimeCountWindows of(Time size, long maxCount) {
        return new TumblingProcessingTimeCountWindows(size.toMilliseconds(), maxCount,0L, WindowStagger.ALIGNED);
    }

    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    public boolean isEventTime() {
        return false;
    }
}
