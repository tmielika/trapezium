package com.verizon.bda.license

object LicenseType extends Enumeration {
  type LicenseType = Value
  val PLATFORM, API = Value
}

case class LicenseException(message: String, cause: Throwable = None.orNull)
              extends Exception(message, cause)

import LicenseType._

object LicenseLib {
  def init(zkServers: String): Unit = {}
  def isValid(licType: LicenseType): Boolean = true
}

