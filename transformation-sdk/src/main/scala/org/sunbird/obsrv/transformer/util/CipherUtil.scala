package org.sunbird.obsrv.transformer.util

import org.sunbird.obsrv.core.model.SystemConfig

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

object CipherUtil {

  private val algorithm = "AES"

  private val encryptInstance = getInstance(Cipher.ENCRYPT_MODE)

  private val decryptInstance = getInstance(Cipher.DECRYPT_MODE)

  def encrypt(value: String): String = {
    if (value.isEmpty) return value
    val encryptedByteValue =  encryptInstance.doFinal(value.getBytes("utf-8"))
    Base64.getEncoder.encodeToString(encryptedByteValue)
  }

  def decrypt(value: String): String = {
    val decryptedValue64 = Base64.getDecoder.decode(value)
    val decryptedByteValue = decryptInstance.doFinal(decryptedValue64)
    new String(decryptedByteValue, "utf-8")
  }

  private def getInstance(mode: Int): Cipher = {
    val cipher = Cipher.getInstance(algorithm)
    val key = new SecretKeySpec(SystemConfig.getString("encryptionSecretKey").getBytes("utf-8"), algorithm)
    cipher.init(mode, key)
    cipher
  }

}