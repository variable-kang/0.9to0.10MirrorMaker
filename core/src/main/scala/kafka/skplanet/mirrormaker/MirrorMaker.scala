package kafka.skplanet.mirrormaker

/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Collections, Properties}

import kafka.skplanet.mirrormaker.clients.producer.{ProducerConfig, ProducerRecord}
import com.yammer.metrics.core.Gauge
import joptsimple.OptionParser
import kafka.consumer._
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.message.MessageAndMetadata
import kafka.metrics.KafkaMetricsGroup
import kafka.serializer.DefaultDecoder
import kafka.utils.{CommandLineUtils, CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.util.control.ControlThrowable

/**
  * The mirror maker has the following architecture:
  * - There are N mirror maker thread shares one ZookeeperConsumerConnector and each owns a Kafka stream.
  * - All the mirror maker threads share one producer.
  * - Each mirror maker thread periodically flushes the producer and then commits all offsets.
  *
  * @note      For mirror maker, the following settings are set by default to make sure there is no data loss:
  *       1. use new producer with following settings
  *            acks=all
  *            retries=max integer
  *            block.on.buffer.full=true
  *            max.in.flight.requests.per.connection=1
  *       2. Consumer Settings
  *            auto.commit.enable=false
  *       3. Mirror Maker Setting:
  *            abort.on.send.failure=true
  */
object MirrorMaker extends Logging with KafkaMetricsGroup {

  private var producer: MirrorMakerProducer = null
  private var mirrorMakerThreads: Seq[MirrorMakerThread] = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  private val numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  private var messageHandler: MirrorMakerMessageHandler = null
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  def main(args: Array[String]) {

    info("Starting mirror maker")
    try {
      val parser = new OptionParser

      val consumerConfigOpt = parser.accepts("consumer.config",
        "Embedded consumer config for consuming from the source cluster.")
        .withRequiredArg()
        .describedAs("config file")
        .ofType(classOf[String])

      val useNewConsumerOpt = parser.accepts("new.consumer",
        "Use new consumer in mirror maker.")

      val producerConfigOpt = parser.accepts("producer.config",
        "Embedded producer config.")
        .withRequiredArg()
        .describedAs("config file")
        .ofType(classOf[String])

      val numStreamsOpt = parser.accepts("num.streams",
        "Number of consumption streams.")
        .withRequiredArg()
        .describedAs("Number of threads")
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(1)

      val whitelistOpt = parser.accepts("whitelist",
        "Whitelist of topics to mirror.")
        .withRequiredArg()
        .describedAs("Java regex (String)")
        .ofType(classOf[String])

      val blacklistOpt = parser.accepts("blacklist",
        "Blacklist of topics to mirror. Only old consumer supports blacklist.")
        .withRequiredArg()
        .describedAs("Java regex (String)")
        .ofType(classOf[String])

      val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
        "Offset commit interval in ms")
        .withRequiredArg()
        .describedAs("offset commit interval in millisecond")
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(60000)

      val consumerRebalanceListenerOpt = parser.accepts("consumer.rebalance.listener",
        "The consumer rebalance listener to use for mirror maker consumer.")
        .withRequiredArg()
        .describedAs("A custom rebalance listener of type ConsumerRebalanceListener")
        .ofType(classOf[String])

      val rebalanceListenerArgsOpt = parser.accepts("rebalance.listener.args",
        "Arguments used by custom rebalance listener for mirror maker consumer")
        .withRequiredArg()
        .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
        .ofType(classOf[String])

      val messageHandlerOpt = parser.accepts("message.handler",
        "Message handler which will process every record in-between consumer and producer.")
        .withRequiredArg()
        .describedAs("A custom message handler of type MirrorMakerMessageHandler")
        .ofType(classOf[String])

      val messageHandlerArgsOpt = parser.accepts("message.handler.args",
        "Arguments used by custom rebalance listener for mirror maker consumer")
        .withRequiredArg()
        .describedAs("Arguments passed to message handler constructor.")
        .ofType(classOf[String])

      val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
        "Configure the mirror maker to exit on a failed send.")
        .withRequiredArg()
        .describedAs("Stop the entire mirror maker when a send failure occurs")
        .ofType(classOf[String])
        .defaultsTo("true")

      val helpOpt = parser.accepts("help", "Print this message.")

      if (args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")


      val options = parser.parse(args: _*)

      if (options.has(helpOpt)) {
        parser.printHelpOn(System.out)
        System.exit(0)
      }

      CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)
      if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
        println("Exactly one of whitelist or blacklist is required.")
        System.exit(1)
      }

      abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
      offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()
      val numStreams = options.valueOf(numStreamsOpt).intValue()

      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run() {
          cleanShutdown()
        }
      })

      // create producer
      val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
      // Defaults to no data loss settings.
      maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
      maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
      // Always set producer key and value serializer to ByteArraySerializer.
      producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "kafka.skplanet.mirrormaker.common.serialization.ByteArraySerializer")
      producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "kafka.skplanet.mirrormaker.common.serialization.ByteArraySerializer")
      producer = new MirrorMakerProducer(producerProps)


      // Create consumers
      val mirrorMakerConsumers /*: Seq[MirrorMakerBaseConsumer] */ = {
        val customRebalanceListener = {
          val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
          if (customRebalanceListenerClass != null) {
            val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
            if (rebalanceListenerArgs != null) {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
            } else {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
            }
          } else {
            None
          }
        }
        if (customRebalanceListener.exists(!_.isInstanceOf[ConsumerRebalanceListener]))
          throw new IllegalArgumentException("The rebalance listener should be an instance of kafka.consumer.ConsumerRebalanceListener")
        createOldConsumers(
          numStreams,
          options.valueOf(consumerConfigOpt),
          customRebalanceListener,
          Option(options.valueOf(whitelistOpt)),
          Option(options.valueOf(blacklistOpt)))
      }

      // Create mirror maker threads.
      mirrorMakerThreads = (0 until numStreams) map (i =>
        new MirrorMakerThread(mirrorMakerConsumers(i), i))

      // Create and initialize message handler
      val customMessageHandlerClass = options.valueOf(messageHandlerOpt)
      val messageHandlerArgs = options.valueOf(messageHandlerArgsOpt)
      messageHandler = {
        if (customMessageHandlerClass != null) {
          if (messageHandlerArgs != null)
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass, messageHandlerArgs)
          else
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass)
        } else {
          defaultMirrorMakerMessageHandler
        }
      }
    } catch {
      case ct: ControlThrowable => throw ct
      case t: Throwable =>
        error("Exception when starting mirror maker.", t)
    }

    mirrorMakerThreads.foreach(_.start())
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  private def createOldConsumers(numStreams: Int,
                                 consumerConfigPath: String,
                                 customRebalanceListener: Option[ConsumerRebalanceListener],
                                 whitelist: Option[String],
                                 blacklist: Option[String]): Seq[MirrorMakerBaseConsumer] = {
    // Create consumer connector
    val consumerConfigProps = Utils.loadProps(consumerConfigPath)
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    val connectors = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      val consumerConfig = new ConsumerConfig(consumerConfigProps)
      new ZookeeperConsumerConnector(consumerConfig)
    }

    // create filters
    val filterSpec = if (whitelist.isDefined)
      new Whitelist(whitelist.get)
    else if (blacklist.isDefined)
      new Blacklist(blacklist.get)
    else
      throw new IllegalArgumentException("Either whitelist or blacklist should be defined!")
    (0 until numStreams) map { i =>
      val consumer = new MirrorMakerOldConsumer(connectors(i), filterSpec)
      val consumerRebalanceListener = new InternalRebalanceListenerForOldConsumer(consumer, customRebalanceListener)
      connectors(i).setConsumerRebalanceListener(consumerRebalanceListener)
      consumer
    }
  }

  def commitOffsets(mirrorMakerConsumer: MirrorMakerBaseConsumer) {
    if (!exitingOnSendFailure) {
      trace("Committing offsets.")
      try {
        mirrorMakerConsumer.commit()
      } catch {
        case e: WakeupException =>
          // we only call wakeup() once to close the consumer,
          // so if we catch it in commit we can safely retry
          // and re-throw to break the loop
          mirrorMakerConsumer.commit()
          throw e
      }
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown() {
    if (isShuttingdown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (mirrorMakerThreads != null) {
        mirrorMakerThreads.foreach(_.shutdown())
        mirrorMakerThreads.foreach(_.awaitShutdown())
      }
      info("Closing producer.")
      producer.close()
      info("Kafka mirror maker shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String) {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run() {
      info("Starting mirror maker thread " + threadName)
      mirrorMakerConsumer.init()
      try {
        // We need the two while loop to make sure when old consumer is used, even there is no message we
        // still commit offset. When new consumer is used, this is handled by poll(timeout).
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown && mirrorMakerConsumer.hasData) {
              val data = mirrorMakerConsumer.receive()
              trace("Sending message with value size %d and offset %d".format(data.value.length, data.offset))
              val records = messageHandler.handle(data)
              records.foreach(producer.send)

              maybeFlushAndCommitOffsets()
            }
          } catch {
            case cte: ConsumerTimeoutException =>
              trace("Caught ConsumerTimeoutException, continue iteration.")
            case we: WakeupException =>
              trace("Caught ConsumerWakeupException, continue iteration.")
          }
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        info("Flushing producer.")
        producer.flush()
        info("Committing consumer offsets.")
        try {
          commitOffsets(mirrorMakerConsumer)
        } catch {
          case e: WakeupException => // just ignore
        }
        info("Shutting down consumer connectors.")
        // we do not need to call consumer.close() since the consumer has already been interrupted
        mirrorMakerConsumer.cleanup()
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingdown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets() {
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        debug("Committing MirrorMaker state automatically.")
        producer.flush()
        commitOffsets(mirrorMakerConsumer)
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown() {
      try {
        info(threadName + " shutting down")
        shuttingDown = true
        mirrorMakerConsumer.stop()
      }
      catch {
        case ie: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case ie: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  private[kafka] trait MirrorMakerBaseConsumer extends BaseConsumer {
    def init()

    def hasData: Boolean
  }

  private class MirrorMakerOldConsumer(connector: ZookeeperConsumerConnector,
                                       filterSpec: TopicFilter) extends MirrorMakerBaseConsumer {
    private var iter: ConsumerIterator[Array[Byte], Array[Byte]] = null

    override def init() {
      // Creating one stream per each connector instance
      val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())
      require(streams.size == 1)
      val stream = streams(0)
      iter = stream.iterator()
    }

    override def hasData = iter.hasNext()

    override def receive(): BaseConsumerRecord = {
      val messageAndMetadata = iter.next()
      BaseConsumerRecord(messageAndMetadata.topic, messageAndMetadata.partition, messageAndMetadata.offset, messageAndMetadata.key, messageAndMetadata.message)
    }

    override def stop() {
      // Do nothing
    }

    override def cleanup() {
      connector.shutdown()
    }

    override def commit() {
      connector.commitOffsets
    }
  }


  private class InternalRebalanceListenerForNewConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForNewConsumer: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener])
    extends org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
      producer.flush()
      commitOffsets(mirrorMakerConsumer)
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsRevoked(partitions))
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsAssigned(partitions))
    }
  }

  private class InternalRebalanceListenerForOldConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForOldConsumer: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]) {
      producer.flush()
      commitOffsets(mirrorMakerConsumer)
      // invoke custom consumer rebalance listener
      customRebalanceListenerForOldConsumer.foreach(_.beforeReleasingPartitions(partitionOwnership))
    }

    override def beforeStartingFetchers(consumerId: String,
                                        partitionAssignment: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]]) {
      customRebalanceListenerForOldConsumer.foreach(_.beforeStartingFetchers(consumerId, partitionAssignment))
    }
  }

  private class MirrorMakerProducer(val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new kafka.skplanet.mirrormaker.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: kafka.skplanet.mirrormaker.clients.producer.ProducerRecord[Array[Byte], Array[Byte]]) {
      if (sync) {
        this.producer.send(record).get()
      } else {
        this.producer.send(record,
          new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush() {
      this.producer.flush()
    }

    def close() {
      this.producer.close()
    }

    def close(timeout: Long) {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
    }
  }

  private class MirrorMakerProducerCallback(topic: String, key: Array[Byte], value: Array[Byte])
    extends kafka.skplanet.mirrormaker.clients.producer.internals.ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: kafka.skplanet.mirrormaker.clients.producer.RecordMetadata, exception: Exception) {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        if (abortOnSendFailure) {
          info("Closing producer due to send failure.")
          exitingOnSendFailure = true
          producer.close(0)
        }
        numDroppedMessages.incrementAndGet()
      }
    }
  }

  /**
    * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
    */
  trait MirrorMakerMessageHandler {
    def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]]

    def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](record.topic, record.key(), record.message()))
    }

    override def handle(record: kafka.consumer.BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](record.topic, record.key, record.value))
    }
  }

}
