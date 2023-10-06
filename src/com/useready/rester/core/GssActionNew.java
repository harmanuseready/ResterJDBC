package com.useready.rester.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedAction;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simba.support.ILogger;
import com.simba.support.LogUtilities;
import com.useready.rester.Settings;

class GssActionNew implements PrivilegedAction<Exception> {

//    private static final Logger LOGGER = Logger.getLogger(GssAction.class.getName());
//  private final PGStream pgStream;
    private final String host;
    private final String user;
    private final String kerberosServerName;
    private final boolean useSpnego;
    private final GSSCredential clientCredentials;
    private final boolean logServerErrorDetail;
    private String url;
    private ILogger iLogger;
    private Settings setting;

    GssActionNew(
            /* PGStream pgStream, */ GSSCredential clientCredentials, String host, String user,
            String kerberosServerName, boolean useSpnego, boolean logServerErrorDetail, String url, ILogger iLogger,Settings setting) {
//    this.pgStream = pgStream;
        this.clientCredentials = clientCredentials;
        this.host = host;
        this.user = user;
        this.kerberosServerName = kerberosServerName;
        this.useSpnego = useSpnego;
        this.logServerErrorDetail = logServerErrorDetail;
        this.url = url;
        this.iLogger = iLogger;
        this.setting=setting;
    }

    private boolean hasSpnegoSupport(GSSManager manager) throws GSSException {
        org.ietf.jgss.Oid spnego = new org.ietf.jgss.Oid("1.3.6.1.5.5.2");
        org.ietf.jgss.Oid[] mechs = manager.getMechs();

        for (Oid mech : mechs) {
            if (mech.equals(spnego)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Exception run() {
        byte[] inToken = new byte[0];
        byte[] outToken = null;
        JsonNode node = null;
        ObjectMapper mapper = new ObjectMapper();
        try {
            GSSManager manager = GSSManager.getInstance();
            GSSCredential clientCreds = null;
            Oid[] desiredMechs = new Oid[1];
            if (clientCredentials == null) {
                if (useSpnego && hasSpnegoSupport(manager)) {
                    desiredMechs[0] = new Oid("1.3.6.1.5.5.2");
                } else {
                    desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
                }
                GSSName clientName = manager.createName(user, GSSName.NT_USER_NAME);
                clientCreds = manager.createCredential(clientName, 8 * 3600, desiredMechs,
                        GSSCredential.INITIATE_AND_ACCEPT);
            } else {
                desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
                clientCreds = clientCredentials;
            }
           

            GSSName serverName = manager.createName(kerberosServerName + "@" + host, GSSName.NT_HOSTBASED_SERVICE);

            GSSContext secContext = manager.createContext(serverName, desiredMechs[0], clientCreds,
                    GSSContext.DEFAULT_LIFETIME);
            secContext.requestMutualAuth(true);
            try {
				String token=getKerberosTokenString(kerberosServerName + "@" + host,iLogger);
				LogUtilities.logInfo("---------Token----------------------"+token, iLogger);
			} catch (AuthenticationException e) {
				// TODO Auto-generated catch block
				LogUtilities.logInfo("---------Token Exception----------------------"+ e.getMessage(),iLogger);
				e.printStackTrace();
			}
            boolean established = false;
            while (!established) {

                LogUtilities.logInfo("---------GSS session established-------------------------", iLogger);

                outToken = secContext.initSecContext(inToken, 0, inToken.length);

                if (outToken != null) {
                    System.out.println("OUT Token - " + outToken.toString());

                    String authorizationHeader = "Negotiate" + " "
                            + java.util.Base64.getEncoder().encodeToString(outToken);

                    System.out.println("Next Authorization header: " + authorizationHeader);

                    CloseableHttpClient closeableHttpClient = HttpClients.createMinimal();

                    HttpGet httpget = new HttpGet(url);

                    httpget.setHeader("Authorization", authorizationHeader);

                    CloseableHttpResponse closeableHttpResponse = closeableHttpClient.execute(httpget);

                    try {

                        BufferedReader ins = null;

                        ins = new BufferedReader(new InputStreamReader(closeableHttpResponse.getEntity().getContent()));

                        // Compressed stream reader to be utilized.
                        String inputLine;
                        StringBuffer response = new StringBuffer();
                        // String test = "";
                        while ((inputLine = ins.readLine()) != null) {

                            response.append(inputLine);
                        }
                        try {
                            ins.close();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        System.out.println(response.toString());
                        LogUtilities.logInfo("---------response------1-------------------------" + response.toString(),
                                iLogger);
                        node = mapper.readTree(response.toString().replaceAll("\\“", "\""));
                        setting.setNode(node);
                        LogUtilities.logInfo(
                                "--------JsonNode -response-----------------------------" + setting.getNode().toString(),
                                iLogger);
                        // return node;
                    } finally {

                        try {
                            closeableHttpResponse.close();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                    }

                    if (!secContext.isEstablished()) {
                        System.out.println("!secContext.isEstablished()");
//                    
//                    }
                    } else {
                        established = true;
                    }
                }
            }

        } catch (GSSException gsse) {
            gsse.printStackTrace();
        } catch (ClientProtocolException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        return null;
    }
    
    public String getKerberosTokenString(String m_service,ILogger serverName) throws GSSException, AuthenticationException

	{

	    String m_tokenStr;

	    String KERBEROS_OID = "1.2.840.113554.1.2.2";

	    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

	    if (null != System.getenv("KRB5_CONFIG"))

	    {
	        System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");

	    }

	    Oid s_kerberosOid = new Oid(KERBEROS_OID);
	    GSSManager m_gssManager = GSSManager.getInstance();
	    GSSContext gssContext = m_gssManager.createContext(
	            m_gssManager.createName(m_service,  GSSName.NT_HOSTBASED_SERVICE).canonicalize(s_kerberosOid),
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
	    gssContext.dispose();           
	    return m_tokenStr;

	}


}