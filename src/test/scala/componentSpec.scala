package com.osinka.camel

import org.specs._
import org.specs.runner._

import com.surftools.BeanstalkClient.{Client => BeanstalkClient}

class componentTest extends JUnit4(componentSpec) with Console
object componentTestRunner extends ConsoleRunner(componentSpec)

object componentSpec extends Specification {
  "BeanstalkComponent" should {
    val component = new BeanstalkComponent

    "parse URI" in {
      import component.parseUri

      parseUri("host.domain.tld:11300/someTube") must be_==(ConnectionSettings("host.domain.tld", 11300, "someTube"))
      parseUri("host.domain.tld/someTube") must be_==(ConnectionSettings("host.domain.tld", BeanstalkClient.DEFAULT_PORT, "someTube"))
      parseUri("someTube") must be_==(ConnectionSettings(BeanstalkClient.DEFAULT_HOST, BeanstalkClient.DEFAULT_PORT, "someTube"))
      parseUri("not_valid@host:port") must throwA[IllegalArgumentException]
    }
  }

}