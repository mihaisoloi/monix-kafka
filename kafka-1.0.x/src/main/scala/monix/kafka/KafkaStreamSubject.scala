package monix.kafka

import java.util
import scala.concurrent.Future
import monix.eval.Task
import monix.execution.{Ack, Cancelable}
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.Subject
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, Produced}
import scala.concurrent.{Future, blocking}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.Serde

/** Exposes an `Observable` that consumes a Kafka stream by
  * means of a Kafka Stream .
  *
  * In order to get initialized, it needs a configuration. See the
  * [[KafkaConsumerConfig]] needed and see `monix/kafka/default.conf`,
  * (in the resource files) that is exposing all default values.
  */
final class KafkaStreamSubject[K, V] private
  (config: KafkaStreamConfig, stream: Task[KStream[K,V]]) extends Subject[K,V] {
  override def size: Int = ???

  override def onNext(elem: K): Future[Ack] = ???

  override def onError(ex: Throwable): Unit = ???

  override def onComplete(): Unit = ???

  override def unsafeSubscribeFn(subscriber: Subscriber[V]): Cancelable = ???
}

object KafkaStreamSubject {
  def apply[K,V](cfg: KafkaStreamConfig, streamTask: Task[KStream[K,V]]): KafkaStreamSubject[K, V] =
    new KafkaStreamSubject[K,V](cfg, streamTask)

  /*
    def apply[K,V](cfg: KafkaConsumerConfig, topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): KafkaConsumerObservable[K,V] = {

    val consumer = createConsumer[K,V](cfg, topics)
    apply(cfg, consumer)
  }

  /** Returns a `Task` for creating a consumer instance. */
  def createConsumer[K,V](config: KafkaConsumerConfig, topics: List[String])
    (implicit K: Deserializer[K], V: Deserializer[V]): Task[KafkaConsumer[K,V]] = {

    import collection.JavaConverters._
    Task {
      val props = config.toProperties
      blocking {
        val consumer = new KafkaConsumer[K,V](props, K.create(), V.create())
        consumer.subscribe(topics.asJava)
        consumer
      }
    }
  }
   */

  def createStream[K,V](config: KafkaStreamConfig, topics: List[String])
    (implicit K: Serde[K], V: Serde[V]): Task[KStream[K,V]] = {
    import collection.JavaConverters._
    Task {
      val props = config.toProperties
      blocking {
        val streamBuilder = new StreamsBuilder()
        streamBuilder.stream(topics.asJava, Consumed.`with`(K.create(), V.create()))
      }
    }
  }
}