import java.io.{InputStream, OutputStream}

import WordCount0.{SimpleDoFn, expected, input}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AtomicCoder, SerializableCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.{Count, DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object WordCount8JPV {

  class ScioContext(args: Array[String]) {
    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)
    def close() = pipeline.run()

    def parallelize[A: ClassTag](input: Iterable[A]): SCollection[A] = {
      val p = pipeline.apply(Create.of(input.asJava))
      new SCollection[A](p)
    }
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }


  class SCollection[A: ClassTag](val internal: PCollection[A]) {
    val ct = implicitly[ClassTag[A]]

    def applyTransform[B: ClassTag](t: PTransform[PCollection[A], PCollection[B]]) : SCollection[B] =
      new SCollection(internal.apply(t))

    def filter(f: A => Boolean): SCollection[A] = flatMap(x => if (f(x)) Some(x) else None )

    def map[B: ClassTag](f: A => B): SCollection[B] = flatMap((x: A) => Some(f(x)))

    def flatMap[B: ClassTag](f: A => TraversableOnce[B]): SCollection[B] = {
      val p = internal.apply(ParDo.of(new SimpleDoFn[A, B]("flatMap") {
        override def process(c: DoFn[A,B]#ProcessContext) =
          f(c.element()).foreach(c.output)
      }))
      val coder = new KryoCoder[B]()
      p.setCoder(coder)
      new SCollection(p)
    }

    def countByValue: SCollection[(A, Long)] =
      applyTransform(Count.perElement()).map[(A, Long)](kv => (kv.getKey, kv.getValue))

  }

  class KryoCoder[A : ClassTag] extends AtomicCoder[A] {
    val kryo = new Kryo()
    val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]

    def encode(value: A, outStream: OutputStream): Unit = {
      val out = new Output(outStream)
      kryo.writeObject(out,value)
    }

    def decode(inStream: InputStream): A = {
      val in = new Input(inStream)
      return kryo.readObject(in, clazz)
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .flatMap(s => s.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .countByValue
      .map(kv => kv._1 + " " + kv._2)

    PAssert.that(result.internal).containsInAnyOrder(expected)

    sc.close()
  }
}
