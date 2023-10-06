package com.useready.rester.core;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.AuthenticationException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.simba.support.ILogger;
import com.simba.support.LogUtilities;


/**
 * Implements PrivilegedAction to support Subject run with AccessControlContext 
 * for Kerberos authentication over http connection.
 */
public class KerberosHttpPrivilegedAction<T> implements PrivilegedExceptionAction<T>
{
    /*
     * Static variable(s) ========================================================================
     */    
    /**
     *  A fair reentrant lock.
     */
    private static ReentrantLock s_kerberosLock;    
    
    /**
     * Kerberos v5 GSS-API mechanism defined in RFC 1964.
     */ 
    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";    
    
    /**
     *  Kerberos Oid.
     */
    private static Oid s_kerberosOid;
    
    protected ILogger m_log;
    
    /*
     * Instance variable(s) ========================================================================
     */
    /**
     * GSS Manager.
     */
    private GSSManager m_gssManager;
    
    /**
     * GSS server name.
     */
    private GSSName m_gssServerName;

    /**
     * Service principal.
     */
    private String m_service;

    /**
     * Kerberos token string.
     */
    private String m_tokenStr;
 
    /*
     * Static Initializer ==========================================================================
     */ 
    /**
     * Static Initializer to initial static variables shared by all the classes's instances.
     */
    static  
    {
        try
        {
            s_kerberosOid = new Oid(KERBEROS_OID);
            s_kerberosLock = new ReentrantLock(true);
        }
        catch (GSSException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }
    
    /*
     * Constructor(s) ==============================================================================
     */
    
    /**
     * Constructor.
     * 
     * @param service           the kerberos service name.
     */    
    public KerberosHttpPrivilegedAction(String service,ILogger log)
    {
    	m_log=log;
        m_gssManager = GSSManager.getInstance();        
        m_service = service;
    }
    
    /**
     * Get the Kerberos token string.
     * 
     * @return Kerberos token string.
     */
    public String getKerberosTokenString()
    {
        return m_tokenStr;
    }

    /**
     * create the transaction
     */
    public T run() throws Exception
    {        
        try
        {  
            // Generate Kerberos token.
            // Locking ensures the tokens are unique in case of concurrent requests.
            s_kerberosLock.lock(); 
            
            GSSContext gssContext = m_gssManager.createContext(
                    m_gssManager.createName(m_service, GSSName.NT_HOSTBASED_SERVICE).canonicalize(s_kerberosOid), 
                    s_kerberosOid, 
                    null,
                    GSSContext.DEFAULT_LIFETIME);
            
            gssContext.requestMutualAuth(true);
            gssContext.requestCredDeleg(true);        
            
            byte[] token = new byte[0];            
            token = gssContext.initSecContext(token, 0, token.length);
            if (null == token) 
            {
                throw new AuthenticationException("GSS security context initialization failed");
            }
          
            m_tokenStr = new String(Base64.encodeBase64(token, false));  
           // LogUtilities.logInfo("--Token generated----"+m_tokenStr, m_log);
            gssContext.dispose();            
        }
        finally 
        {
            s_kerberosLock.unlock();
        }
        return null;
    }
}
