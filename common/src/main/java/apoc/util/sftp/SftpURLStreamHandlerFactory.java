package apoc.util.sftp;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class SftpURLStreamHandlerFactory implements URLStreamHandlerFactory {
    
    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        return new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL url) {
                return new SftpURLConnection(url);
            }
        };
    }
}
