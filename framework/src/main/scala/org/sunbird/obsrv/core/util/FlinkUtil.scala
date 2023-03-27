package org.sunbird.obsrv.core.util

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.sunbird.obsrv.core.streaming.BaseJobConfig
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

object FlinkUtil {

  def getExecutionContext(config: BaseJobConfig[_]): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setUseSnapshotCompression(config.enableCompressedCheckpointing)
    env.enableCheckpointing(config.checkpointingInterval)

    /**
      * Use Blob storage as distributed state backend if enabled
      */
    // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked with a cloud blob store config
    config.enableDistributedCheckpointing match {
      case Some(true) => {
        val stateBackend: StateBackend = new FsStateBackend(s"${config.checkpointingBaseUrl.getOrElse("")}/${config.jobName}", true)
        env.setStateBackend(stateBackend)
        val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        checkpointConfig.setMinPauseBetweenCheckpoints(config.checkpointingPauseSeconds     )
      }
      case _ => // Do nothing
    }
    // $COVERAGE-ON$

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.restartAttempts, config.delayBetweenAttempts))
    env
  }
}