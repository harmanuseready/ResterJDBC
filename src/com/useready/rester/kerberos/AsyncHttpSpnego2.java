package com.useready.rester.kerberos;
//package com.useready.rester.core;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.net.InetAddress;
//import java.security.AccessController;
//import java.security.PrivilegedAction;
//import java.util.Base64;
//import java.util.Set;
//
//import javax.security.auth.Subject;
//import javax.security.auth.login.LoginContext;
//
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.ietf.jgss.GSSContext;
//import org.ietf.jgss.GSSCredential;
//import org.ietf.jgss.GSSException;
//import org.ietf.jgss.GSSManager;
//import org.ietf.jgss.GSSName;
//import org.ietf.jgss.Oid;
//
//public class AsyncHttpSpnego2 {
//
//    public static void authenticate(String host, String user, String password,
//            String jaasApplicationName, String kerberosServerName, boolean useSpnego, boolean jaasLogin,
//            boolean logServerErrorDetail)
//            throws IOException {
//
//        Exception result;
//        try {
//            boolean performAuthentication = jaasLogin;
//            GSSCredential gssCredential = null;
//            Subject sub = Subject.getSubject(AccessController.getContext());
//            if (sub != null) {
//                Set<GSSCredential> gssCreds = sub.getPrivateCredentials(GSSCredential.class);
//                if (gssCreds != null && !gssCreds.isEmpty()) {
//                    gssCredential = gssCreds.iterator().next();
//                    performAuthentication = false;
//                }
//            }
////            if (performAuthentication) {
////              LoginContext lc =
////                  new LoginContext(jaasApplicationName, new GSSCallbackHandler(user, password));
////              lc.login();
////              sub = lc.getSubject();
////            }
////            PrivilegedAction<Exception> action = new GssAction(gssCredential, host, user,
////                kerberosServerName, useSpnego, logServerErrorDetail);
//
////            result = Subject.doAs(sub, action);
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        }
//
////          if (result instanceof IOException) {
////            throw (IOException) result;
////          } else if (result instanceof SQLException) {
////            throw (SQLException) result;
////          } else if (result != null) {
////              System.out.println(result);
////          }
//
//    }
//
////    public static void authenticate(
////            /* PGStream pgStream, */ String host, String user, String password,
////            String jaasApplicationName, String kerberosServerName, boolean useSpnego, boolean jaasLogin,
////            boolean logServerErrorDetail)
//
//    public static void mains(String[] args) throws Exception {
//        authenticate("kdz.useready.com", "tableau_admin", "", "rester", "HTTP", true, false, false);
//    }
//
//    public static final String SPNEGO_OID = "1.3.6.1.5.5.2";
//
//    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";
//
//    public static void main(String[] args) throws Exception {
//
//        InetAddress inetAddress = InetAddress.getLocalHost();
//
//        String host = "https://kdz.useready.com/dz/collibra/getEntitledDatasetsDetails";
//
//        System.setProperty("java.security.auth.login.config", "C:\\Program Files\\Tableau\\Drivers\\login.conf");
//        System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");
//
////        System.setProperty("java.security.krb5.conf", new File(host + "-krb5.ini").getCanonicalPath());
//
//        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
//
////        System.setProperty("java.security.auth.login.config", new File(host + "-login.conf").getCanonicalPath());
////        LoginContext lc = new LoginContext("KrbLogin");
////
////        lc.login();
//        boolean performAuthentication = false;
//        GSSCredential gssCredential = null;
//        Subject sub = Subject.getSubject(AccessController.getContext());
//        if (sub != null) {
//            Set<GSSCredential> gssCreds = sub.getPrivateCredentials(GSSCredential.class);
//            if (gssCreds != null && !gssCreds.isEmpty()) {
//                gssCredential = gssCreds.iterator().next();
//                performAuthentication = false;
//            }
//        }
//        if (performAuthentication) {
//            LoginContext lc = new LoginContext("KrbLogin", new GSSCallbackHandler(user, password));
//            lc.login();
//            sub = lc.getSubject();
//        }
//        PrivilegedAction<Exception> action = new GssAction(pgStream, gssCredential, host, user,
//                kerberosServerName, useSpnego, logServerErrorDetail);
//
//        result = Subject.doAs(sub, action);
//
//        byte[] token = new byte[0];
//
//        token = getAuthToken("kdz.useready.com".toUpperCase(), lc, "tableau_admin@USEREADY.COM");
//
//        String authorizationHeader = "Negotiate" + " " + Base64.getEncoder().encodeToString(token);
//
//        System.out.println("Next Authorization header: " + authorizationHeader);
//
//        CloseableHttpClient closeableHttpClient = HttpClients.createMinimal();
//
//        HttpGet httpget = new HttpGet(host);
//
//        httpget.setHeader("Authorization", authorizationHeader);
//
//        CloseableHttpResponse closeableHttpResponse = closeableHttpClient.execute(httpget);
//
//        try {
//
//            BufferedReader ins = null;
//
//            ins = new BufferedReader(new InputStreamReader(closeableHttpResponse.getEntity().getContent()));
//
//            // Compressed stream reader to be utilized.
//            String inputLine;
//            StringBuffer response = new StringBuffer();
//            // String test = "";
//            while ((inputLine = ins.readLine()) != null) {
//
//                response.append(inputLine);
//            }
//            ins.close();
//            System.out.println(response.toString());
//
//        } finally {
//
//            closeableHttpResponse.close();
//
//        }
//
//    }
//
//    private static byte[] getAuthToken(String host, LoginContext lc, String username)
//            throws GSSException, java.security.PrivilegedActionException {
//        byte[] inToken = new byte[0];
//        byte[] outToken = null;
//        try {
//            GSSManager manager = GSSManager.getInstance();
//            GSSCredential clientCreds = null;
//            Oid[] desiredMechs = new Oid[1];
//            desiredMechs[0] = new Oid("1.3.6.1.5.5.2");
//            GSSName clientName = manager.createName(username, GSSName.NT_USER_NAME);
//            clientCreds = manager.createCredential(clientName, 8 * 3600, desiredMechs,
//                    GSSCredential.INITIATE_ONLY);
//
//            GSSName serverName = manager.createName("HTTP" + "@" + host, GSSName.NT_HOSTBASED_SERVICE);
//
//            GSSContext secContext = manager.createContext(serverName, desiredMechs[0], clientCreds,
//                    GSSContext.DEFAULT_LIFETIME);
//            secContext.requestMutualAuth(true);
//
//            boolean established = false;
//            while (!established) {
//                outToken = secContext.initSecContext(inToken, 0, inToken.length);
//
//                if (outToken != null) {
//                    System.out.println("OUT Token - " + outToken.toString());
//                }
//
//                if (!secContext.isEstablished()) {
//                    System.out.println("!secContext.isEstablished()");
//                } else {
//                    established = true;
//                    System.out.println("!secContext.isEstablished() TRUE");
//                }
//            }
//
//        } catch (GSSException gsse) {
//            gsse.printStackTrace();
//        }
//        return outToken;
//    }
//
//}