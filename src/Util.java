import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

public class Util {

    static void writeContents(File file, Object... contents) {
        /**
         * Write the content into file
         *  Args:
         *      file: a File object of location
         *      contents: the material that is written in the file
         */
        try {
            if (file.isDirectory()) {
                throw
                        new IllegalArgumentException("cannot overwrite directory");
            }
            BufferedOutputStream str =
                    new BufferedOutputStream(Files.newOutputStream(file.toPath()));
            for (Object obj : contents) {
                if (obj instanceof byte[]) {
                    str.write((byte[]) obj);
                } else {
                    str.write(((String) obj).getBytes(StandardCharsets.UTF_8));
                }
            }
            str.close();
        } catch (IOException | ClassCastException excp) {
            throw new IllegalArgumentException(excp.getMessage());
        }
    }

    static <T extends Serializable> T readObject(File file,
                                                 Class<T> expectedClass) {
        /**
         * Read the content into file
         *  Args:
         *      file: a File object of location
         *      expectedClass: the class of the object read
         *  Return:
         *      the object read from the file
         */
        try {
            ObjectInputStream in =
                    new ObjectInputStream(new FileInputStream(file));
            T result = expectedClass.cast(in.readObject());
            in.close();
            return result;
        } catch (IOException | ClassCastException
                | ClassNotFoundException excp) {
            throw new IllegalArgumentException(excp.getMessage());
        }
    }
    static void writeObject(File file, Serializable obj) {
        writeContents(file, serialize(obj));
    }

    static byte[] serialize(Serializable obj) {
        /** serialize an object
         *  args: anything that is serializable
         *  return : list of bytes of serializable
         */
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ObjectOutputStream objectStream = new ObjectOutputStream(stream);
            objectStream.writeObject(obj);
            objectStream.close();
            return stream.toByteArray();
        } catch (IOException excp) {
            throw new IllegalArgumentException("Internal error serializing.");
        }
    }

    static <T extends Serializable> T deserialize(byte[] obj, Class<T> expectedClass){
        /** deserialize an object
         *  args:
         *      obj: the byte list of object
         *      expectedClass: the expected class of serializable object
         *  return : the original object
         */
        try{
            ByteArrayInputStream stream = new ByteArrayInputStream(obj);
            ObjectInputStream in = new ObjectInputStream(stream);
            T result = expectedClass.cast(in.readObject());
            in.close();
            return result;
        }catch(IOException | ClassNotFoundException excp){
            throw new IllegalArgumentException("Internal error deserializing.");
        }
    }

    static File join(File first, String... others) {
        /** join the path with name
         * args:
         *      first: the directory of the file
         *      others: the name of the file
         * return: the composite result of the location of file
         */
        return Paths.get(first.getPath(), others).toFile();
    }

}
