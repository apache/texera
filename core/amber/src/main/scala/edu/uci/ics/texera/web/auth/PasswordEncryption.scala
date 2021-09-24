package edu.uci.ics.texera.web.auth

import org.jasypt.util.password.StrongPasswordEncryptor

object PasswordEncryption {
  def encrypt(str: String): String = {
    val passwordEncryptor = new StrongPasswordEncryptor
    passwordEncryptor.encryptPassword(str)
  }
  def checkPassword(encryptedPassword: String, inputPassword: String): Boolean = {
    val passwordEncryptor = new StrongPasswordEncryptor
    passwordEncryptor.checkPassword(inputPassword, encryptedPassword)
  }
}
