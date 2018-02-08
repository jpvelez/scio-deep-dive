package serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

// No need to extend Serializable!
class Person {
  public String name = "John Doe";
  public int age = 18;
  private Date birthDate = new Date(933191282821L);
}

public class KryoSerialization {
  public static void main(String[] args) throws IOException {

    Person p = new Person();
    String path = "kryoObj.bin";

    Output out = new Output(new FileOutputStream(path));
    Kryo k = new Kryo();
    k.writeObject(out, p);
    k.writeObject(out, new Date(915170400000L));
    out.close();

    Input in = new Input(new FileInputStream(path));
    Person pOut = k.readObject(in, Person.class); // Uses Kryo's FieldSerializer by default.
    Date dateOut = k.readObject(in, Date.class);
    in.close();

    System.out.println("Object has been deserialized:");
    System.out.println(pOut.name);
    System.out.println(pOut.age);
    System.out.println(dateOut);
  }
}
