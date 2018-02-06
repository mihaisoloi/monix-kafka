/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package monix.kafka

import java.nio.ByteBuffer
import org.apache.kafka.common.serialization.Serdes.{ByteArraySerde, ByteBufferSerde, BytesSerde, DoubleSerde, IntegerSerde, LongSerde, StringSerde}
import org.apache.kafka.common.serialization.{Serde => KafkaSerde}
import org.apache.kafka.common.utils.Bytes

/** Wraps a Kafka `Serde`, provided for
  * convenience, since it can be implicitly fetched
  * from the context.
  *
  * @param className   is the full package path to the Kafka `Serde`
  * @param classType   is the `java.lang.Class` for [[className]]
  * @param constructor creates an instance of [[classType]].
  *                    This is defaulted with a `Serde.Constructor[A]` function that creates a
  *                    new instance using an assumed empty constructor.
  *                    Supplying this parameter allows for manual provision of the `Serde`.
  */
final case class Serde[A](
  className: String,
  classType: Class[_ <: KafkaSerde[A]],
  constructor: Serde.Constructor[A] = (sd: Serde[A]) => sd.classType.newInstance()
) {

  /** Creates a new instance. */
  def create(): KafkaSerde[A] =
    constructor(this)
}

object Serde {

  /** Alias for the function that provides an instance of
    * the Kafka `Serde`.
    */
  type Constructor[A] = (Serde[A]) => KafkaSerde[A]

  implicit val forStrings: Serde[String] =
    Serde[String](
      className = "org.apache.kafka.common.serialization.Serdes.StringSerde",
      classType = classOf[StringSerde]
    )

  implicit val forByteArray: Serde[Array[Byte]] =
    Serde[Array[Byte]](
      className = "org.apache.kafka.common.serialization.Serdes.ByteArraySerde",
      classType = classOf[ByteArraySerde]
    )

  implicit val forByteBuffer: Serde[ByteBuffer] =
    Serde[ByteBuffer](
      className = "org.apache.kafka.common.serialization.Serdes.ByteBufferSerde",
      classType = classOf[ByteBufferSerde]
    )

  implicit val forBytes: Serde[Bytes] =
    Serde[Bytes](
      className = "org.apache.kafka.common.serialization.Serdes.BytesSerde",
      classType = classOf[BytesSerde]
    )

  implicit val forJavaDouble: Serde[java.lang.Double] =
    Serde[java.lang.Double](
      className = "org.apache.kafka.common.serialization.Serdes.DoubleSerde",
      classType = classOf[DoubleSerde]
    )

  implicit val forJavaInteger: Serde[java.lang.Integer] =
    Serde[java.lang.Integer](
      className = "org.apache.kafka.common.serialization.Serdes.IntegerSerde",
      classType = classOf[IntegerSerde]
    )

  implicit val forJavaLong: Serde[java.lang.Long] =
    Serde[java.lang.Long](
      className = "org.apache.kafka.common.serialization.Serdes.LongSerde",
      classType = classOf[LongSerde]
    )
}
