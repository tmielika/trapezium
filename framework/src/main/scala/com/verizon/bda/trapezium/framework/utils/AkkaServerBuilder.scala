package com.verizon.bda.trapezium.framework.utils

import java.security.{KeyStore, SecureRandom}

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import com.oath.auth.{KeyRefresher, Utils}
import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.server.{AkkaServer, AkkaTlsServer, EmbeddedServer}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import org.apache.log4j.Logger

object AkkaServerBuilder {
  lazy val log = Logger.getLogger(this.getClass)
  def build(serverConfig: Config): EmbeddedServer = {
    val enableHttps = serverConfig.getBoolean("enableHttps")
    if (!enableHttps) {
      log.info("Starting in HTTP mode")
      new AkkaServer
    }
    else {
      log.info("Starting in HTTPS mode")
      val https: HttpsConnectionContext = HttpsConnectionContextBuilder.build(serverConfig)
      new AkkaTlsServer(httpsContext = https)
    }
  }
}

object HttpsConnectionContextBuilder {
  lazy val log = Logger.getLogger(this.getClass)

  def build(httpServerConfig: Config): HttpsConnectionContext = {
    val oathEnv = httpServerConfig.getBoolean("oathEnv")
    val certPath = httpServerConfig.getString("certPath")
    val keyPath = httpServerConfig.getString("keyPath")
    val trustStorePath = if (oathEnv) {
      httpServerConfig.getString("trustStorePath")
    } else {
      ""
    }
    val trustStorePassword = if (oathEnv) {
      httpServerConfig.getString("trustStorePassword")
    } else {
      ""
    }

    val passwd = s"secret"

    val keystore = Utils.createKeyStore(certPath, keyPath)

    require(keystore != null, "Keystore required!")

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")

    keyManagerFactory.init(keystore, passwd.toCharArray)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    if (oathEnv) {
      val ksTrust = KeyStore.getInstance("JKS")
      val trustStoreResource = this.getClass.getClassLoader.getResourceAsStream(trustStorePath)
      ksTrust.load(trustStoreResource, trustStorePassword.toCharArray)
      tmf.init(ksTrust)
    }
    else {
      tmf.init(keystore)
    }

    val sslContext = if (oathEnv) {
      createSSLContext(certPath, keyPath, trustStorePath, trustStorePassword)
    }
    else {
      SSLContext.getInstance("TLS")
    }
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)

    ConnectionContext.https(sslContext)
  }

  def createSSLContext(certPath: String, keyPath: String,
                       trustStorePath: String, trustStorePassword: String): SSLContext = {
    try {
      log.info("certPath=" + certPath)
      log.info("keyPath=" + keyPath)
      log.info("trustStorePath=" + trustStorePath)
      val keyRefresher: KeyRefresher = Utils.generateKeyRefresher(trustStorePath, trustStorePassword,
        certPath, keyPath)
      // Default refresh period is every hour.
      keyRefresher.startup()
      // Can be adjusted to use other values in milliseconds.
      // keyRefresher.startup(900000);
      Utils.buildSSLContext(keyRefresher.getKeyManagerProxy,
        keyRefresher.getTrustManagerProxy())
    }
    catch {
      case th: Throwable =>
        log.error("sslcontext not formed", th)
        null
    }
  }
}

