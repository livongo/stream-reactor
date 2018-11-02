/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.mqtt.sink

import java.util.Base64

import org.json4s._
import org.json4s.DefaultReaders._
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.{FieldConverter, Transform}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSinkSettings
import com.datamountaineer.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.json4s.native.JsonMethods
//import org.json4s._
import org.json4s.jackson.JsonMethods._
//import org.json4s.DefaultReaders._
import org.json4s.native.JsonMethods


import scala.util.parsing.json._
import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 27/08/2017. 
  * stream-reactor
  */

object MqttWriter {
  def apply(settings: MqttSinkSettings): MqttWriter = {
    new MqttWriter(MqttClientConnectionFn.apply(settings), settings)
  }
}

class MqttWriter(client: MqttClient, settings: MqttSinkSettings)
  extends StrictLogging with ErrorHandler {
  implicit val formats = DefaultFormats
  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)
  val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)
  val kcql = settings.kcql
  val msg = new MqttMessage()
  msg.setQos(settings.mqttQualityOfService)

  def write(records: Set[SinkRecord]) = {

    val grouped = records.groupBy(r => r.topic())

    val t = Try(grouped.map({
      case (topic, records) =>
        //get the kcql statements for this topic
        val kcqls: Set[Kcql] = mappings.get(topic).get
        kcqls.map(k => {
          //for all the records in the group transform
          records.map(r => {
            val trans = Transform(
              k.getFields.map(FieldConverter.apply),
              k.getIgnoredFields.map(FieldConverter.apply),
              r.valueSchema(),
              r.value(),
              k.hasRetainStructure
            )
            val target = (JsonMethods.parse(trans) \ "target").getAsOrElse[String](k.getTarget)
            (target, trans)
          }).map(
            {
              case (t, jsonString) => {
                val json = parse(jsonString)
                val payload = (json \ "payload").getAsOrElse("")
                val asbytes = Base64.getDecoder.decode(payload)
                msg.setPayload(asbytes)
                client.publish(t, msg)
              }
            }
          )
        })
    }))

    //handle any errors according to the policy.
    handleTry(t)
  }

  def flush = {}

  def close = {
    client.disconnect()
    client.close()
  }
}
