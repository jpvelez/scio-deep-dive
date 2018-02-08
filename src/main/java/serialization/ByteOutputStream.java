package serialization;

import java.io.*;


public class ByteOutputStream {

  public static void main(String args[]) throws IOException {
    File f = new File("test.bin");
    FileOutputStream fop = new FileOutputStream(f);

    String content = "Hello World!";
    byte[] contentInBytes = content.getBytes();

    fop.write(contentInBytes);
    fop.flush();
    fop.close();
  }
}
