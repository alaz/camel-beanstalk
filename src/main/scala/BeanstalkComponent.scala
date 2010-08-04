package com.osinka.camel

import org.apache.camel.{Producer, Consumer, Processor, Endpoint}
import org.apache.camel.impl.{DefaultComponent, DefaultEndpoint}
import com.surftools.BeanstalkClient.{Client => BeanstalkClient}
import com.surftools.BeanstalkClientImpl.{ClientImpl => BeanstalkClientImpl}
import scala.reflect.BeanProperty

case class ConnectionSettings(val host: String, val port: Int, val tube: String)

/**
 * URI format:
 * beanstalk://hostName:port/tubeName
 * beanstalk://hostName/tubeName
 * beanstalk://tubeName
 */
class BeanstalkComponent extends DefaultComponent {
  val HostPortTubeRE = """([\w\d.]+):([\d]+)/(.+)""".r
  val HostTubeRE = """([\w\d.]+)/(.+)""".r
  val TubeRE = """(.+)""".r

  def createEndpoint(uri: String, remaining: String, parameters: java.util.Map[String, Object]): Endpoint = {
    def addTube(client: BeanstalkClientImpl, tube: String) = {
      client.useTube(tube)
      client
    }

    val conn = parseUri(remaining)
    new BeanstalkEndpoint(uri, this, addTube(new BeanstalkClientImpl(conn.host, conn.port), conn.tube))
  }

  def parseUri(remaining: String) = remaining match {
      case HostPortTubeRE(host, port, tube) => ConnectionSettings(host, port.toInt, tube)
      case HostTubeRE(host, tube) => ConnectionSettings(host, BeanstalkClient.DEFAULT_PORT, tube)
      case TubeRE(tube) => ConnectionSettings(BeanstalkClient.DEFAULT_HOST, BeanstalkClient.DEFAULT_PORT, tube)
      case _ => throw new IllegalArgumentException("Invalid path format: %s - should be <hostName>:<port>/<tubeName>" format remaining)
    }
}

class BeanstalkEndpoint(uri: String, component: BeanstalkComponent, client: BeanstalkClient) extends DefaultEndpoint(uri, component) {
  /**
   * Blocking of client thread during two-way message exchanges with consumer actors. This is set
   * via the <code>blocking=true|false</code> endpoint URI parameter. If omitted blocking is false.
   */
  @BeanProperty var blocking: Boolean = false

  def isSingleton = true

  def createProducer: Producer = throw new Exception("")

  def createConsumer(processor: Processor): Consumer = throw new Exception("")
}