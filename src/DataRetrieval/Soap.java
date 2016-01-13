package DataRetrieval;

import org.ksoap2.SoapEnvelope;
import org.ksoap2.serialization.SoapObject;
import org.ksoap2.serialization.SoapSerializationEnvelope;
import org.ksoap2.transport.HttpTransportSE;
import org.kxml2.io.KXmlParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;

/**
 * Created by KheyaliMitra on 12/25/2015.
 */
public class Soap {

    public final String WSDL_TARGET_NAMESPACE = "http://tempuri.org/";

    final static HostnameVerifier DO_NOT_VERIFY = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /**
     * Get JSOn string from the output format
     * @param URL SOAP address URL
     * @param OperationName Name of the operation
     * @param Parameters HashMap of the parameters
     * @return
     * @throws XmlPullParserException
     * @throws IOException
     */
    public String GetJSONString(String URL, String OperationName, HashMap<String, Object> Parameters) throws XmlPullParserException, IOException {
        //Initialize soap request object
        SoapObject request = new SoapObject(WSDL_TARGET_NAMESPACE, OperationName);
        if (Parameters != null) {
            //Sets all operations and parameters
            for (HashMap.Entry<String, Object> entry : Parameters.entrySet()) {
                request.addProperty(entry.getKey(), entry.getValue());
            }
        }

        //Creates the envelope
        SoapSerializationEnvelope envelope = this._createEnvelope(request);

        //Get URL connection
        HttpURLConnection conn = this._getConnection(URL, true);

        HttpTransportSE httpTransport = new HttpTransportSE(conn.getURL().toString());
        String res = null;
        try {
            httpTransport.call(WSDL_TARGET_NAMESPACE + OperationName, envelope);
            res = envelope.getResponse().toString();
            try {
                XmlPullParser parser = new KXmlParser();
                parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, false);
                InputStream in = new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return 0;
                    }
                };

            } catch (Exception exception) {
                String text = exception.toString();
            }

        } catch (Exception e) {
            int x = 0;

        }
        return res;
    }

    /**
     * Creates envelope from request object
     * @param request
     * @return
     */
    private SoapSerializationEnvelope _createEnvelope(SoapObject request) {
        //Creates the envelope
        SoapSerializationEnvelope envelope = new SoapSerializationEnvelope(
                SoapEnvelope.VER11);
        envelope.dotNet = true;
        envelope.setOutputSoapObject(request);
        return envelope;
    }

    /**
     * Gets connection irrespective of HTTP or HTTPS
     * @param HostURL
     * @param IsByPassTrustedHost
     * @return
     * @throws IOException
     */
    private HttpURLConnection _getConnection(String HostURL, Boolean IsByPassTrustedHost) throws IOException {
        URL url = new URL(HostURL);
        if (url.getProtocol().toLowerCase().equals("https")) {
            if (IsByPassTrustedHost) {
                trustAllHosts();
            }
            HttpsURLConnection https = (HttpsURLConnection) url.openConnection();
            if (IsByPassTrustedHost) {
                https.setHostnameVerifier(DO_NOT_VERIFY);
            }
            return https;
        } else {
            return (HttpURLConnection) url.openConnection();
        }
    }

    private static void trustAllHosts() {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[]{};
            }

            public void checkClientTrusted(X509Certificate[] chain,
                                           String authType) throws CertificateException {
            }

            public void checkServerTrusted(X509Certificate[] chain,
                                           String authType) throws CertificateException {
            }
        }};

        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection
                    .setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
