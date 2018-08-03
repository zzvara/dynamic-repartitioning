package hu.sztaki.drc.utilities

import java.util.Base64

import org.apache.commons.lang3.SerializationUtils
import org.slf4j.LoggerFactory

trait Logger {
  private val serializedObjectPrefix = "|||"

  @transient private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(
    this.getClass.getName.stripSuffix("$")
  ))

  protected def logObject(any: Serializable) = {
    if (logger.underlying.isDebugEnabled) {
      val serialized = SerializationUtils.serialize(any)
      logger.info(serializedObjectPrefix + new String(Base64.getEncoder.encode(serialized)))
    }
  }

  protected def logInfo(message: => String): Unit = logger.info(message)
  protected def logInfo(message: => String,
                        throwable: Throwable): Unit = logger.info(message, throwable)

  protected def logWarning(message: => String): Unit = logger.warn(message)
  protected def logWarning(message: => String,
                           throwable: Throwable): Unit = logger.warn(message, throwable)

  protected def logError(message: => String): Unit = logger.error(message)
  protected def logError(message: => String,
                         throwable: Throwable): Unit = logger.error(message, throwable)

  protected def logDebug(message: => String): Unit = logger.debug(message)
  protected def logDebug(message: => String,
                         throwable: Throwable): Unit = logger.debug(message, throwable)

  protected def logTrace(message: => String): Unit = logger.trace(message)
  protected def logTrace(message: => String,
                         throwable: Throwable): Unit = logger.trace(message, throwable)
}