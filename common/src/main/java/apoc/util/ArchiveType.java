package apoc.util;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import java.io.InputStream;

public enum ArchiveType {
    NONE(null),
    ZIP(ZipArchiveInputStream.class),
    TAR(TarArchiveInputStream.class);

    private final Class<?> stream;

    ArchiveType(Class<?> stream) {
        this.stream = stream;
    }

    public static ArchiveType from(String urlAddress) {
        if (!urlAddress.contains("!")) {
            return NONE;
        }
        if (urlAddress.contains(".zip")) {
            return ZIP;
        } else if (urlAddress.contains(".tar") || urlAddress.contains(".tgz")) {
            return TAR;
        }
        return NONE;
    }
    
    public ArchiveInputStream getInputStream(InputStream is) {
        try { 
            return (ArchiveInputStream) stream.getConstructor(InputStream.class).newInstance(is);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isArchive() {
        return stream != null;
    }
}
