package hu.sztaki.drc.utilities

import com.typesafe.config.Config
import hu.sztaki.drc.StreamingDecider

object Factory {
  abstract class default[T]() extends Serializable {
    def apply(): T
  }
  abstract class withConfiguration[T]() extends Serializable {
    def apply(configuration: Config): T
  }
  abstract class forStreamingDecider[Stream <: { def numPartitions: Int }] extends Serializable {
    def apply(streamID: Int,
              stream: Stream,
//              numPartitions: Int,
              perBatchSamplingRate: Int = 1,
              resourceStateHandler: Option[() => Int] = None): StreamingDecider[Stream]
  }
}
