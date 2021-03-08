package edu.uci.ics.texera.web.resource.auth;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import sun.misc.BASE64Encoder;

public class PasswordEncryption {
    public static String EncoderByMd5(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException{
        // the way to encrypt the password
        MessageDigest md5=MessageDigest.getInstance("MD5");
        BASE64Encoder base64en = new BASE64Encoder();

        String newstr=base64en.encode(md5.digest(str.getBytes("utf-8")));
        return newstr;
    }

}
