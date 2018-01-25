package com.xwtech.util

import com.typesafe.config.ConfigFactory

object LocationConfig {
  private val config = ConfigFactory.load("location")

  lazy val kafkaSourceTopic = config.getString("kafkaSourceTopic")

  lazy val kafkaBrokers = config.getString("kafkaBrokers")

  lazy val groupId = config.getString("groupId")

  lazy val hTable = config.getString("hTable")

  lazy val zkHost = config.getString("zkHost")
}
