package org.sunbird.obsrv.denormalizer.util;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;
    private final ReducingStateDescriptor<Long> stateDesc;

    private CountProcessingTimeTrigger(long maxCount) {
        this.stateDesc = new ReducingStateDescriptor<>("count", new CountProcessingTimeTrigger.Sum(), LongSerializer.INSTANCE);
        this.maxCount = maxCount;
    }

    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        ReducingState<Long> count = ctx.getPartitionedState(this.stateDesc);
        count.add(1L);
        if ((Long) count.get() >= this.maxCount) {
            count.clear();
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) {
        return TriggerResult.FIRE;
    }

    public void clear(TimeWindow window, Trigger.TriggerContext ctx) {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        ((ReducingState) ctx.getPartitionedState(this.stateDesc)).clear();
    }

    public boolean canMerge() {
        return true;
    }

    public void onMerge(TimeWindow window, Trigger.OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
        ctx.mergePartitionedState(this.stateDesc);
    }

    public String toString() {
        return "ProcessingTimeTrigger()";
    }

    public static CountProcessingTimeTrigger of(long maxCount) {
        return new CountProcessingTimeTrigger(maxCount);
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        private Sum() {
        }

        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }
}
