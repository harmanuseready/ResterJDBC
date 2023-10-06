package com.useready.rester.kerberos;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

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

public class AsyncHttpSpnego {

	public static final String SPNEGO_OID = "1.3.6.1.5.5.2";
	private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";

	public static void main(String[] args) throws Exception {

		InetAddress inetAddress = InetAddress.getLocalHost();

		String host = "https://kdz.useready.com/dz/collibra/getEntitledDatasetsDetails";

		System.setProperty("java.security.auth.login.config", "C:\\Program Files\\Tableau\\Drivers\\login.conf");
		System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");

//        System.setProperty("java.security.krb5.conf", new File(host + "-krb5.ini").getCanonicalPath());

		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

//        System.setProperty("java.security.auth.login.config", new File(host + "-login.conf").getCanonicalPath());
		LoginContext lc = new LoginContext("KrbLogin");

		lc.login();

		byte[] token = new byte[0];

		token = getAuthToken("kdz.useready.com".toUpperCase(), lc, token);

		String authorizationHeader = "Negotiate" + " " + Base64.getEncoder().encodeToString(token);

		System.out.println("Next Authorization header: " + authorizationHeader);

		CloseableHttpClient closeableHttpClient = HttpClients.createMinimal();

		HttpGet httpget = new HttpGet(host);

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
			ins.close();
			System.out.println(response.toString());

		} finally {

			closeableHttpResponse.close();

		}

	}

	private static byte[] getAuthToken(String host, LoginContext lc, byte[] inToken)
			throws GSSException, java.security.PrivilegedActionException {

		Oid negotiationOid = new Oid(SPNEGO_OID);

		GSSManager manager = GSSManager.getInstance();

		final PrivilegedExceptionAction<GSSCredential> action = () -> manager.createCredential(null,

				GSSCredential.INDEFINITE_LIFETIME, negotiationOid, GSSCredential.INITIATE_AND_ACCEPT);

		boolean tryKerberos = false;

		GSSContext gssContext = null;

		try {

			GSSName serverName = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);

			gssContext = manager.createContext(serverName.canonicalize(negotiationOid), negotiationOid,
					Subject.doAs(lc.getSubject(), action),
					// referring postgres jar replace above line with following
//                    Subject.doAs(Subject.getSubject(AccessController.getContext()), action),

					GSSContext.DEFAULT_LIFETIME);

			gssContext.requestMutualAuth(true);

			gssContext.requestCredDeleg(true);

		} catch (GSSException ex) {

			if (ex.getMajor() == GSSException.BAD_MECH) {

				System.out.println("GSSException BAD_MECH, retry with Kerberos MECH");

				tryKerberos = true;

			} else {

				throw ex;

			}

		}

		if (tryKerberos) {

			Oid kerbOid = new Oid(KERBEROS_OID);

			GSSName serverName = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);

			gssContext = manager.createContext(serverName.canonicalize(kerbOid), kerbOid,
					Subject.doAs(lc.getSubject(), action),

					GSSContext.DEFAULT_LIFETIME);

			gssContext.requestMutualAuth(true);

			gssContext.requestCredDeleg(true);

		}

		return gssContext.initSecContext(inToken, 0, inToken.length);

	}

}