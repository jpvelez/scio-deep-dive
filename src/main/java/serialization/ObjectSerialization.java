package serialization;

import java.io.*;

class Demo implements Serializable {
  int a;
  String b;

  public Demo(int a, String b){
    this.a = a;
    this.b = b;
  }
}

public class ObjectSerialization {
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String path = "demoObj.bin";
    FileOutputStream fos = new FileOutputStream(path);
    Demo obj = new Demo(1, "hi there");
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(obj);

    FileInputStream fis = new FileInputStream(path);
    ObjectInputStream ois = new ObjectInputStream(fis);
    Demo objOut = (Demo)ois.readObject();
    System.out.println("Object has been deserialized:" + objOut.a);
    System.out.println(objOut.a);
    System.out.println(objOut.b);
  }
}
