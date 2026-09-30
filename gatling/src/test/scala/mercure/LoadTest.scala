/** Load test for Mercure.
  *
  * Run the load test: ./mvnw gatling:test
  *
  * Available environment variables (all optional):
  *   - HUB_URL: the URL of the hub to test
  *   - JWT: the JWT to use for authenticating the publisher
  *   - SUBSCRIBER_JWT: the JWT to use for authenticating the subscriber,
  *     fallbacks to JWT if not set and PRIVATE_UPDATES set
  *   - SUBSCRIBER_HEADERS: extra request headers sent on subscribe requests
  *     only, as `Name: value` pairs separated by newlines or `;`. Must not
  *     name Authorization (use SUBSCRIBER_JWT / JWT)
  *   - PUBLISHER_HEADERS: extra request headers sent on publish requests only,
  *     same format. Must not name Authorization (use JWT)
  *   - INITIAL_SUBSCRIBERS: the number of concurrent subscribers initially
  *     connected
  *   - SUBSCRIBERS_RATE_FROM: minimum rate (per second) of additional
  *     subscribers to connect
  *   - SUBSCRIBERS_RATE_TO: maximum rate (per second) of additional subscribers
  *     to connect
  *   - PUBLISHERS_RATE_FROM: minimum rate (per second) of publications
  *   - PUBLISHERS_RATE_TO: maximum rate (per second) of publications
  *   - INJECTION_DURATION: duration of the publishers injection
  *   - CONNECTION_DURATION: duration of subscribers' connection
  *   - RANDOM_CONNECTION_DURATION: to randomize the connection duration (will
  *     longs CONNECTION_DURATION at max)
  *   - PRIVATE_UPDATES: send private updates with random topics (default false)
  *   - SUBSCRIBE_PARAM: overrides the subscribe query parameter, e.g.
  *     "match=https://example.com"
  *   - PUBLISH_TOPIC: overrides the published topic; it must match the
  *     subscriber's matcher
  */

package mercure

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import scala.util.Properties

/** Parser for the SUBSCRIBER_HEADERS and PUBLISHER_HEADERS environment
  * variables.
  */
object RequestHeadersParser {

  /** Parses `Name: value` pairs separated by newlines or `;`. Each pair is
    * split on its first `:`; names and values are trimmed; blank entries are
    * skipped.
    *
    * Called at simulation start, so a malformed entry or an Authorization
    * header fails fast with an IllegalArgumentException naming `varName` and
    * the 1-based position of the entry among the non-blank entries. The entry
    * text is never echoed, as it may hold a token. `tokenSource` is the clause
    * appended to the Authorization error, saying where the token comes from
    * instead (e.g. "the publisher token comes from JWT").
    */
  def parse(
      varName: String,
      tokenSource: String,
      raw: String
  ): List[(String, String)] = {
    raw
      .split("[\\r\\n;]")
      .toList
      .filter(_.trim.nonEmpty)
      .zipWithIndex
      .map { case (entry, i) =>
        val idx = entry.indexOf(':')
        val name = if (idx < 0) "" else entry.substring(0, idx).trim
        if (name.isEmpty) {
          throw new IllegalArgumentException(
            "Invalid " + varName + " entry " + (i + 1) +
              ": expected 'Name: value'"
          )
        }
        if (name.equalsIgnoreCase("Authorization")) {
          throw new IllegalArgumentException(
            "Invalid " + varName + " entry " + (i + 1) +
              ": Authorization is not allowed; " + tokenSource
          )
        }
        (name, entry.substring(idx + 1).trim)
      }
  }
}

class LoadTest extends Simulation {

  /** The hub URL */
  val HubUrl =
    Properties.envOrElse("HUB_URL", "https://localhost/.well-known/mercure")

  /** JWT to use to publish */
  val Jwt = Properties.envOrElse(
    "JWT",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6ImF0K2p3dCJ9.eyJhdWQiOiJodHRwczovL2xvY2FsaG9zdC8ud2VsbC1rbm93bi9tZXJjdXJlIiwiYXV0aG9yaXphdGlvbl9kZXRhaWxzIjpbeyJhY3Rpb25zIjpbInB1Ymxpc2giXSwidG9waWNzIjpbeyJtYXRjaCI6IioifV0sInR5cGUiOiJodHRwczovL21lcmN1cmUucm9ja3MvYXV0aG9yaXphdGlvbi1kZXRhaWwifSx7ImFjdGlvbnMiOlsic3Vic2NyaWJlIl0sInRvcGljcyI6W3sibWF0Y2giOiIqIn1dLCJ0eXBlIjoiaHR0cHM6Ly9tZXJjdXJlLnJvY2tzL2F1dGhvcml6YXRpb24tZGV0YWlsIn1dLCJleHAiOjQxMDI0NDQ4MDAsImlzcyI6Imh0dHBzOi8vbG9jYWxob3N0In0.VO0-PRjJ2MGOrMk2HxlrBv217pB7hyLxLIQUGgSfyXs"
  )

  /** JWT to use to subscribe, fallbacks to JWT if not set and PRIVATE_UPDATES
    * set
    */
  val SubscriberJwt = Properties.envOrElse("SUBSCRIBER_JWT", null)

  /** Number of concurrent subscribers initially connected */
  val InitialSubscribers =
    Properties.envOrElse("INITIAL_SUBSCRIBERS", "100").toInt

  /** Additional subscribers rate (per second) */
  val SubscribersRateFrom =
    Properties.envOrElse("SUBSCRIBERS_RATE_FROM", "2").toInt
  val SubscribersRateTo =
    Properties.envOrElse("SUBSCRIBERS_RATE_TO", "10").toInt

  /** Publishers rate (per second) */
  val PublishersRateFrom =
    Properties.envOrElse("PUBLISHERS_RATE_FROM", "2").toInt
  val PublishersRateTo = Properties.envOrElse("PUBLISHERS_RATE_TO", "20").toInt

  /** Duration of injection (in seconds) */
  val InjectionDuration =
    Properties.envOrElse("INJECTION_DURATION", "3600").toInt

  /** How long a subscriber can stay connected at max (in seconds) */
  val ConnectionDuration =
    Properties.envOrElse("CONNECTION_DURATION", "300").toInt

  /** Randomize the connection duration? */
  val RandomConnectionDuration =
    Properties.envOrElse("RANDOM_CONNECTION_DURATION", "true").toBoolean

  /** Send private updates with random topics instead of public ones always with
    * the same topic
    */
  var PrivateUpdates =
    Properties.envOrElse("PRIVATE_UPDATES", "false").toBoolean

  /** Override the subscribe matcher query parameter, e.g.
    * "match=https://example.com" (exact) or
    * "match_urlpattern=https://example.com/:id" (URL Pattern). Lets a run
    * target a specific matcher type without editing this file; defaults to a
    * value derived from PRIVATE_UPDATES.
    */
  val SubscribeParam = Properties.envOrElse("SUBSCRIBE_PARAM", null)

  /** Override the published topic. Must stay matchable by the subscriber's
    * matcher, otherwise the delivery check times out. Defaults to a value
    * derived from PRIVATE_UPDATES.
    */
  val PublishTopic = Properties.envOrElse("PUBLISH_TOPIC", null)

  /** Extra headers added to subscribe requests only. */
  val SubscriberHeaders = RequestHeadersParser.parse(
    "SUBSCRIBER_HEADERS",
    "the subscriber token comes from SUBSCRIBER_JWT / JWT",
    Properties.envOrElse("SUBSCRIBER_HEADERS", "")
  )

  /** Extra headers added to publish requests only. */
  val PublisherHeaders = RequestHeadersParser.parse(
    "PUBLISHER_HEADERS",
    "the publisher token comes from JWT",
    Properties.envOrElse("PUBLISHER_HEADERS", "")
  )

  val rnd = new scala.util.Random

  /** Subscriber test as a function to handle conditional Authorization header
    */
  def subscriberTest() = {
    // Public updates share one exact topic; private updates use random topics
    // under a URL Pattern. SUBSCRIBE_PARAM overrides both.
    var param = "match=https://example.com"
    if (PrivateUpdates) {
      param = "match_urlpattern=https://example.com/:id"
    }
    if (SubscribeParam != null) {
      param = SubscribeParam
    }

    var requestBuilder = sse("Subscribe").get("?" + param)

    if (SubscriberJwt != null) {
      requestBuilder =
        requestBuilder.header("Authorization", "Bearer " + SubscriberJwt)
    } else if (PrivateUpdates) {
      requestBuilder = requestBuilder.header("Authorization", "Bearer " + Jwt)
    }

    SubscriberHeaders.foreach { case (name, value) =>
      requestBuilder = requestBuilder.header(name, value)
    }

    requestBuilder.await(10)(
      sse.checkMessage("Check content").check(regex("""(.*)Hi(.*)"""))
    )
  }

  val httpProtocol = http
    .baseUrl(HubUrl)

  var topic = "https://example.com"
  if (PrivateUpdates) {
    topic = topic + "/" + rnd.nextInt()
  }
  if (PublishTopic != null) {
    topic = PublishTopic
  }

  var data = Map("topic" -> topic, "data" -> "Hi")
  if (PrivateUpdates) {
    data = data + ("private" -> "true")
  }

  val publishRequest = PublisherHeaders.foldLeft(
    http("Publish")
      .post("")
      .header("Authorization", "Bearer " + Jwt)
      .formParamMap(data)
      .check(status.is(200))
  ) { case (request, (name, value)) => request.header(name, value) }

  val scenarioPublish = scenario("Publish").exec(publishRequest)

  val scenarioSubscribe = scenario("Subscribe")
    .exec(
      subscriberTest()
    )
    .pause(
      if (RandomConnectionDuration) rnd.nextInt(ConnectionDuration)
      else ConnectionDuration
    )
    .exec(sse("Close").close)

  setUp(
    scenarioSubscribe
      .inject(
        atOnceUsers(InitialSubscribers),
        rampUsersPerSec(
          SubscribersRateFrom
        ) to SubscribersRateTo during (InjectionDuration seconds) randomized
      )
      .protocols(httpProtocol),
    scenarioPublish
      .inject(
        rampUsersPerSec(
          PublishersRateFrom
        ) to PublishersRateTo during (InjectionDuration + ConnectionDuration seconds) randomized
      )
      .protocols(httpProtocol)
  )
}
