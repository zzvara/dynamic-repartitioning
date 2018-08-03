package hu.sztaki.drc.utilities

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

class Configuration {
  private val configuration = ConfigFactory.load("repartitioning.conf")

  def internal: Config = configuration
}

object Configuration extends Logger {
  private var _configuration: Option[Configuration] = None

  def get(): Configuration = {
    _configuration.getOrElse {
      logInfo("Creating configuration for the first time.")
      _configuration = Some(new Configuration)
      _configuration.get
    }
  }

  def internal(): Config = {
    get().internal
  }

  def getOption: Option[Configuration] = {
    _configuration
  }
}