package monix.kafka

import java.io.File
import java.util.Properties
import com.typesafe.config.{Config, ConfigFactory}
import monix.kafka.config._
import scala.concurrent.duration._
import org.apache.kafka.streams.StreamsConfig._
import org.apache.kafka.common.serialization.{Serde, Serdes}

final case class KafkaStreamConfig(
  // HIGH
  applicationId: String,
  bootstrapServers: List[String],
  replicationFactor: Int,
  stateDir: String,
  // MEDIUM
  cacheMaxBytesBuffer: Long,
  clientId: String,
  defaultKeySerde: Serde[Any],
  defaultValueSerde: Serde[Any],
  defaultDeserializationExceptionHandler: String,
  processingGuarantee: String,
  numStandbyReplicas: Int,
  numStreamThreads: Int,
  securityProtocol: String
) {

  def toMap: Map[String,String] = Map(
    "bootstrap.servers" -> bootstrapServers.mkString(","),
    "client.id" -> clientId
  )

  def toProperties: Properties = {
    val props = new Properties()
    for ((k,v) <- toMap; if v != null) props.put(k,v)
    props
  }
}

object KafkaStreamConfig {
  private val defaultRootPath = "kafka"

  lazy private val defaultConf: Config =
    ConfigFactory.load("monix/kafka/default.conf").getConfig(defaultRootPath)

  /** Returns the default configuration, specified the `monix-kafka` project
    * in `monix/kafka/default.conf`.
    */
  lazy val default: KafkaProducerConfig =
    apply(defaultConf, includeDefaults = false)

  /** Loads the [[KafkaProducerConfig]] either from a file path or
    * from a resource, if `config.file` or `config.resource` are
    * defined, or otherwise returns the default config.
    *
    * If you want to specify a `config.file`, you can configure the
    * Java process on execution like so:
    * {{{
    *   java -Dconfig.file=/path/to/application.conf
    * }}}
    *
    * Or if you want to specify a `config.resource` to be loaded
    * from the executable's distributed JAR or classpath:
    * {{{
    *   java -Dconfig.resource=com/company/mySpecial.conf
    * }}}
    *
    * In case neither of these are specified, then the configuration
    * loaded is the default one, from the `monix-kafka` project, specified
    * in `monix/kafka/default.conf`.
    */
  def load(): KafkaProducerConfig =
    Option(System.getProperty("config.file")).map(f => new File(f)) match {
      case Some(file) if file.exists() =>
        loadFile(file)
      case None =>
        Option(System.getProperty("config.resource")) match {
          case Some(resource) =>
            loadResource(resource)
          case None =>
            default
        }
    }

  /** Loads a [[KafkaProducerConfig]] from a project resource.
    *
    * @param resourceBaseName is the resource from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadResource(resourceBaseName: String, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.load(resourceBaseName).getConfig(rootPath), includeDefaults)

  /** Loads a [[KafkaProducerConfig]] from a specified file.
    *
    * @param file is the configuration path from where to load the config
    * @param rootPath is the config root path (e.g. `kafka`)
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def loadFile(file: File, rootPath: String = defaultRootPath, includeDefaults: Boolean = true): KafkaProducerConfig =
    apply(ConfigFactory.parseFile(file).resolve().getConfig(rootPath), includeDefaults)

  /** Loads the [[KafkaProducerConfig]] from a parsed
    * `com.typesafe.config.Config` reference.
    *
    * NOTE that this method doesn't assume any path prefix for loading the
    * configuration settings, so it does NOT assume a root path like `kafka`.
    * In case case you need that, you can always do:
    *
    * {{{
    *   KafkaProducerConfig(globalConfig.getConfig("kafka"))
    * }}}
    *
    * @param source is the typesafe `Config` object to read from
    * @param includeDefaults should be `true` in case you want to fallback
    *        to the default values provided by the `monix-kafka` library
    *        in `monix/kafka/default.conf`
    */
  def apply(source: Config, includeDefaults: Boolean = true): KafkaProducerConfig = {
    val config = if (!includeDefaults) source else source.withFallback(defaultConf)
    def getOptString(path: String): Option[String] =
      if (config.hasPath(path)) Option(config.getString(path))
      else None

    KafkaProducerConfig(
      bootstrapServers = config.getString("bootstrap.servers").trim.split("\\s*,\\s*").toList,
      acks = Acks(config.getString("acks")),
      bufferMemoryInBytes = config.getInt("buffer.memory"),
      compressionType = CompressionType(config.getString("compression.type")),
      retries = config.getInt("retries"),
      sslKeyPassword = getOptString("ssl.key.password"),
      sslKeyStorePassword = getOptString("ssl.keystore.password"),
      sslKeyStoreLocation = getOptString("ssl.keystore.location"),
      sslTrustStorePassword = getOptString("ssl.truststore.password"),
      sslTrustStoreLocation = getOptString("ssl.truststore.location"),
      batchSizeInBytes = config.getInt("batch.size"),
      clientId = config.getString("client.id"),
      connectionsMaxIdleTime = config.getInt("connections.max.idle.ms").millis,
      lingerTime = config.getInt("linger.ms").millis,
      maxBlockTime = config.getInt("max.block.ms").millis,
      maxRequestSizeInBytes = config.getInt("max.request.size"),
      partitionerClass = getOptString("partitioner.class").filter(_.nonEmpty).map(PartitionerName.apply),
      receiveBufferInBytes = config.getInt("receive.buffer.bytes"),
      requestTimeout = config.getInt("request.timeout.ms").millis,
      saslKerberosServiceName = getOptString("sasl.kerberos.service.name"),
      saslMechanism = config.getString("sasl.mechanism"),
      securityProtocol = SecurityProtocol(config.getString("security.protocol")),
      sendBufferInBytes = config.getInt("send.buffer.bytes"),
      sslEnabledProtocols = config.getString("ssl.enabled.protocols").split("\\s*,\\s*").map(SSLProtocol.apply).toList,
      sslKeystoreType = config.getString("ssl.keystore.type"),
      sslProtocol = SSLProtocol(config.getString("ssl.protocol")),
      sslProvider = getOptString("ssl.provider"),
      sslTruststoreType = config.getString("ssl.truststore.type"),
      reconnectBackoffTime = config.getInt("reconnect.backoff.ms").millis,
      retryBackoffTime = config.getInt("retry.backoff.ms").millis,
      metadataMaxAge = config.getInt("metadata.max.age.ms").millis,
      metricReporters = config.getString("metric.reporters").trim.split("\\s*,\\s*").toList,
      metricsNumSamples = config.getInt("metrics.num.samples"),
      metricsSampleWindow = config.getInt("metrics.sample.window.ms").millis,
      monixSinkParallelism = config.getInt("monix.producer.sink.parallelism")
    )
  }
}
