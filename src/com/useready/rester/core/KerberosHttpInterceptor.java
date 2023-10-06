package com.useready.rester.core;

import java.io.IOException;
import javax.security.auth.Subject;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import com.simba.support.ILogger;
import com.simba.support.LogUtilities;

/**
 * Authentication interceptor which adds Base64 encoded,
 * kerberos token to the outgoing http request header.
 */
public class KerberosHttpInterceptor implements HttpRequestInterceptor 
{
    /**
     * Authenticated Kerberos Subject
     */
    private Subject m_kerberosSubject;
    
    /**
     * PrivilegedAction as Kerberos authenticated subject.
     */
    private KerberosHttpPrivilegedAction m_action; 
    
    public ILogger iLogger;
    /*
     * Constructor(s) ==============================================================================
     */
    
    /**
     * Constructor.
     * 
     * @param service           the service name.
     * @param host              the host name.
     * @param kerberosSubject   the kerberos subject that does the PrivilegedAction.
     */     
    public KerberosHttpInterceptor(
            String host, 
            Subject kerberosSubject,ILogger log)
    {
    	 iLogger=log;
       m_kerberosSubject = kerberosSubject;    
       m_action = new KerberosHttpPrivilegedAction("HTTP@" + host,iLogger);
      
    }
    
    /**
     * Processes a request. This step is performed before the request is sent to the server 
     * to add Kerberos Negotiate header.
     * 
     * @param request           the request to preprocess
     * @param context           the context for the request
     * 
     * @throws HttpException    in case of an HTTP protocol violation
     * @throws IOException      in case of an I/O error
     */
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException 
    {
        try
        {
            // TODO: If cookie based authentication is allowed, generate ticket only when necessary.
            // The necessary condition is either when there are no server side cookies in the
            // cookiestore which can be send back or when the server returns a 401 error code
            // indicating that the previous cookie has expired.
            
            addHttpAuthHeader(request);     
        } 
        catch (Exception e) 
        {
        	LogUtilities.logInfo("--Process Exception-------"
					+ e.getMessage(), iLogger);
            throw new HttpException(e.getMessage(), e);
        }
    }
        
    /**
     * Add Kerberos Negotiate header to the request before it is sent to the server.
     * 
     * @param request           the request to process
     */    
    protected void addHttpAuthHeader(HttpRequest request) 
        throws Exception 
    {
    	
        Subject.doAs(m_kerberosSubject, m_action);
        request.addHeader("Authorization", "Negotiate " + m_action.getKerberosTokenString());
//        LogUtilities.logInfo("--Add Kerberos Negotiate header ----"
//				+  m_action.getKerberosTokenString(), iLogger);
    }    
}
