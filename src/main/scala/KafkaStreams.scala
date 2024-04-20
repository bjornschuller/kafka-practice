import KafkaStreams.Domain.{Discount, Order, OrderId, Payment, Profile, UserId}
import KafkaStreams.Topics._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.{Decoder, Encoder, syntax}
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._  // this enables a Serde to be convert to a Consumed data type

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/**
 * KAFKA in a NUTSHELL
 * Source: emits (send) elements
 * Flow: transforms elements along the way (e.g. the map function)
 * Sink: ingests (takes) elements
 *    - Just like: Akka streams ;)
 *    - Sources, Flows, and Streams are called stream processors
 *    - A streaming application is just a composition of these sort of elements

 * KTABLE => EXPLANATION
 * Data Ingestion: Data flows into a Kafka stream from a topic.
 *                 This data typically consists of key-value pairs.
 * Processing: As data moves through the Kafka stream, it undergoes various transformations and operations
 *            defined by the application. These operations can include filtering, mapping, aggregating, joining, etc.
 * KTable Interaction: When data passes through a KTable, the KTable essentially keeps track of the latest value associated with each key.
 *                     If the incoming data contains an update for a key that already exists in the KTable, the KTable will update its internal state with the new value.
 * Stateful Processing: KTable enables stateful processing in Kafka Streams. It maintains state about the data it has seen so far,
 *                      allowing it to provide real-time views of the data and support interactive queries.
 * Queryable State: One key feature of KTable is that it provides queryable state.
 *                  This means that applications can perform point lookups to retrieve
 *                  the current value associated with a specific key in real-time.
 * When using a KTable the config must be: "cleanup.policy=compact" ->
 *  kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact"

 *  Each topic in kafka has partitions (bookshelf's). Each partition stores a subset of the data in the topic.
 *  KTable - is distributed.
 *           This means that the data in the KTable is spread out among the partitions,
 *           allowing for parallel processing and scalability.
 * Global KTable - is copied to all the nodes in the cluster.
 *                  The entire dataset is copied to all the nodes in the Kafka cluster.
 *                  This allows every node to have a complete copy of the data, enabling efficient
 *                  lookups and joins across the entire dataset.
 *                  (so this should store a few values, otherwise you destroy your cluster)
 *                  also needs: --config "cleanup.policy=compact"


 */
object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Int)
    case class Discount(profile: Profile, amount: Int)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    final val OrdersByUser = "orders-by-user"
    final val DiscountProfilesByUser = "discount-profiles-by-user"
    final val Discounts = "discounts"
    final val Orders = "orders"
    final val Payments = "payments"
    final val PaidOrders = "paid-orders"
  }

/**
   * Make our Data structures compatible with Kafka streams by serialize and
   * deserialize them to arrays of bytes that kafka can then handle
   * And we're using circe to turn our data structures to json strings
   * and then turn that into bytes
   * Serdes are used to convert data between Kafka's binary format and the corresponding Scala objects.
   * REMEMBER:
   *  An implicit val is evaluated eagerly AS SOON AS IT IS DEFINED
   *  An implicit def is evaluated lazily, only when the compiler requires an implicit value of the corresponding type.

   * when you import io.circe.generic.auto._ in your Scala code,
   * you gain the ability to convert case classes into JSON format and vice versa
   * without having to write the conversion logic yourself.
   * This import relies on macros, which are a way to automatically generate code based
   * on the structure of your case classes
 */
  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
  val serializer = (a: A) => a.asJson.noSpaces.getBytes()
  // Option since deserialization can fail
  val deserializer = (bytes: Array[Byte]) => {
    val string = new String(bytes)
    decode[A](string).toOption
  }

  Serdes.fromFn[A](serializer, deserializer)
}

  // a topology which will describe how the data flows through the kafka stream
  // is a kafka stream's stateful data structure that will allow us to add kafka streams components to this topology
  val builder = new StreamsBuilder()
  val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](Topics.OrdersByUser)
                                                          .peek((k, v) => println(s"TESTT GOT ${k}, ${v}"))


  private def paidOrdersTopology(): Unit = {
    val userProfilesTable: KTable[UserId, Profile] =
      builder.table[UserId, Profile](Topics.DiscountProfilesByUser)

    val discountProfilesGTable: GlobalKTable[Profile, Discount] =
      builder.globalTable[Profile, Discount](Topics.Discounts)

    val ordersWithUserProfileStream: KStream[UserId, (Order, Profile)] =
      usersOrdersStreams.join[Profile, (Order, Profile)](userProfilesTable) { (order, profile) =>
        (order, profile)
      }

    val discountedOrdersStream: KStream[UserId, Order] =
      ordersWithUserProfileStream.join[Profile, Discount, Order](discountProfilesGTable)(
        { case (_, (_, profile)) => profile }, // Joining key
        { case ((order, _), discount) => order.copy(amount = order.amount * discount.amount) }
      )

    val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey { (_, order) => order.orderId }

    val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](Topics.Payments)

    val paidOrders: KStream[OrderId, Order] = {

      val joinOrdersAndPayments = (order: Order, payment: Payment) =>
        if (payment.status == "PAID") Option(order) else Option.empty[Order]

      val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

      ordersStream.join[Payment, Option[Order]](paymentsStream)(joinOrdersAndPayments, joinWindow)
        .flatMapValues(maybeOrder => maybeOrder.toIterable)
    }

    paidOrders.to(Topics.PaidOrders)
  }

  def simpleOrdersTopology(): Unit = {
    val orderIdToUpperCaseStream: KStream[UserId, String] =
      usersOrdersStreams.map{ (user, order) => (user, order.orderId.toUpperCase())}
        .peek((k,v) => println("Transformed event: "+v))

    orderIdToUpperCaseStream.to(Topics.PaidOrders)
  }


  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-practice") // BE AWARE MUST BE THE SAME AS THE NAME IN THE BUILD.sbt
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    //    paidOrdersTopology()
    simpleOrdersTopology();

    val topology: Topology = builder.build()

    println(topology.describe())

    val application: KafkaStreams = new KafkaStreams(topology, props)

    application.start()

    Thread.sleep(12000)
  }


}
