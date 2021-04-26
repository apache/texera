package edu.uci.ics.texera.web.resource.auth;

import org.jasypt.util.password.StrongPasswordEncryptor;

public class PasswordEncryption {
    public static String encrypt(String str){
        StrongPasswordEncryptor passwordEncryptor = new StrongPasswordEncryptor();
        return passwordEncryptor.encryptPassword(str);
    }

    public static boolean checkPassword(String encryptedPassword, String inputPassword) {
        StrongPasswordEncryptor passwordEncryptor = new StrongPasswordEncryptor();
        return passwordEncryptor.checkPassword(inputPassword, encryptedPassword);
    }
}
