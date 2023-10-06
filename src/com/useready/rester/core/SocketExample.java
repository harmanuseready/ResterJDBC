package com.useready.rester.core;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.AuthenticationException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.simba.support.ILogger;

import java.net.*;
//from   j  a v a 2 s .  co m
public class SocketExample {
  public static void main(String[] args) throws Exception {
	 String token= getKerberosTokenString("HTTP@kzinc.useready.com",null);
	 System.out.println("Token+++++++++++" +token);
    InetAddress addr = InetAddress.getByName("kzinc.useready.com"
    		+ "");
    Socket socket = new Socket(addr, 80);
    boolean autoflush = true;
    PrintWriter out = new PrintWriter(socket.getOutputStream(), autoflush);
    BufferedReader in = new BufferedReader(

    new InputStreamReader(socket.getInputStream()));
    // send an HTTP request to the web server
    out.println("GET / HTTP/1.1");
    out.println("Authorization: Negotiate "+token);
    //out.println("Host: kzinc.useready.com:80");
    out.println("Host: kzinc.useready.com\r\n");
    //out.println("");
    
    out.println("Connection: Close");
    out.println();
    PrintWriter write = new PrintWriter(socket.getOutputStream(), autoflush);
    
    BufferedReader ins = new BufferedReader(
    new InputStreamReader(socket.getInputStream()));
    // send an HTTP request to the web server
    write.println("GET / HTTP/1.1");
    write.println("Authorization: Negotiate "+token);
    //out.println("Host: kzinc.useready.com:80");
    write.println("Host: kzinc.useready.com");
    //out.println("");
    
    write.println("Connection: Close");
    write.println();
    // read the response
    boolean loop = true;
    StringBuilder sb = new StringBuilder(8096);
    while (loop) {
      if (in.ready()) {
        int i = 0;
        while (i != -1) {
          i = in.read();
          sb.append((char) i);
        }
        loop = false;
      }
    }
    System.out.println(sb.toString());
    boolean loop1 = true;
    StringBuilder sb1 = new StringBuilder(8096);
    while (loop1) {
      if (ins.ready()) {
        int i = 0;
        while (i != -1) {
          i = ins.read();
          sb1.append((char) i);
        }
        loop = false;
      }
    }
    System.out.println(sb1.toString());
    
    socket.close();
  }
  
  
  public static String getKerberosTokenString(String m_service,ILogger logger) throws GSSException, AuthenticationException

	{
	  System.setProperty("java.security.auth.login.config", "C:\\Program Files\\Tableau\\Drivers\\login.conf");
		System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		System.setProperty("sun.security.krb5.debug", "true");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
	 String m_tokenStr;
	    String KERBEROS_OID = "1.2.840.113554.1.2.2";
	    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
	    if (null != System.getenv("KRB5_CONFIG"))
	    {
	        System.setProperty("java.security.krb5.conf", System.getenv("KRB5_CONFIG"));
	    }
	    System.out.println("getKerberosTokenString+++++++++++");
	    Oid s_kerberosOid = new Oid(KERBEROS_OID);
	    GSSManager m_gssManager = GSSManager.getInstance();
	    GSSContext gssContext = m_gssManager.createContext(
	            m_gssManager.createName(m_service, GSSName.NT_HOSTBASED_SERVICE).canonicalize(s_kerberosOid), 
	            s_kerberosOid,null,
	            GSSContext.DEFAULT_LIFETIME);
	    
	    gssContext.requestMutualAuth(true);
	    gssContext.requestCredDeleg(true);        
	    byte[] token = new byte[0];  
	    try{
	              
	    token = gssContext.initSecContext(token, 0, token.length);
	    if (null == token) 
	    {
	    	
	        throw new AuthenticationException("GSS security context initialization failed");
	    }
	    }catch(Exception e) {
	    	System.out.println("Exception+++++++"+e.getMessage());
	    }
	  
	    m_tokenStr = new String(Base64.encodeBase64(token));           
	    gssContext.dispose();            
	    
	    System.out.println("--------------"+m_tokenStr);
	    
	    return m_tokenStr;


	}
}