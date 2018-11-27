package com.verizon.bda.trapezium.framework.utils

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import com.typesafe.config.Config
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

object HttpsConnectionContextBuilder {
  def build(config: Config): HttpsConnectionContext = {

    val passwd = config.getString("storePasswd")
    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val storeLocation = config.getString("storeLocation")
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream(storeLocation)

    require(keystore != null, "Keystore required!")

    val password: Array[Char] = passwd.toCharArray // do not store passwords in code!
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)

    ConnectionContext.https(sslContext)
  }
}
