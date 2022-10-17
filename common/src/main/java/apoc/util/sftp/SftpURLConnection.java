package apoc.util.sftp;

import apoc.util.StreamConnection;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class SftpURLConnection extends URLConnection {

    @Override
    public void connect() {}

    public SftpURLConnection(URL url) {
        super(url);
    }

    public static StreamConnection openSftpInputStream(URL url) {
        try {
            JSch jsch = new JSch();
            String username = null;
            String password = null;
            final String userInfo = url.getUserInfo();
            if (userInfo != null) {
                String[] credentials = userInfo.split(":");
                username = credentials[0];
                password = credentials[1];
            }
            final Session session = jsch.getSession(username, url.getHost(), url.getPort());
            // todo: StrictHostKeyChecking could be configurable
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);

            session.connect();

            final ChannelSftp channelSftp = (ChannelSftp) session.openChannel(url.getProtocol());
            channelSftp.connect();

            final InputStream inputStream = channelSftp.get(url.getFile());

            return new StreamConnection() {
                @Override
                public InputStream getInputStream() {
                    return inputStream;
                }

                @Override
                public String getEncoding() {
                    return "";
                }

                @Override
                public long getLength() {
                    try {
                        return inputStream.available();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public String getName() {
                    return url.getFile();
                }
            };
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }
 
}
