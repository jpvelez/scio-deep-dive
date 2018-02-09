import java.io.{InputStream, OutputStream}

import WordCount0.{SimpleDoFn, expected, input}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.KryoSerializer
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AtomicCoder, SerializableCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.{Count, DoFn, ParDo}
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object WordCount9JPV {

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
      val cls = implicitly[ClassTag[B]].runtimeClass.asInstanceOf[Class[B]]
      val coder = internal.getPipeline.getCoderRegistry.getCoder(cls)
      if (coder.getClass != classOf[SerializableCoder[_]]) {
        p.setCoder(coder)
      } else {
        println((s"Using KryoAtomicCoder of type $cls"))
        p.setCoder(new KryoAtomicCoder[B]())
      }
      new SCollection(p)
    }

    def countByValue: SCollection[(A, Long)] =
      applyTransform(Count.perElement()).map[(A, Long)](kv => (kv.getKey, kv.getValue))

  }

  implicit def SCollectionToPair[A: ClassTag,B: ClassTag](s: SCollection[(A,B)]): PairSCollection[A,B] =
    new PairSCollection[A,B](s.internal)



  class PairSCollection[A: ClassTag, B: ClassTag](internal: PCollection[(A,B)]) extends SCollection(internal) {

    def groupByKey(): SCollection[(A, Iterable[B])] = {
        map[KV[A,B]] { case (k,v) => KV.of(k,v) }
        .applyTransform(GroupByKey.create())
        .map(kv => (kv.getKey, kv.getValue.asScala))
      }
    }

  class KryoAtomicCoder[A : ClassTag] extends AtomicCoder[A] {
    val kryo: ThreadLocal[Kryo] = new EmptyOnDeserializationThreadLocal[Kryo] {
      override def initialValue(): Kryo = KryoSerializer.registered.newKryo()
    }

    def encode(value: A, outStream: OutputStream): Unit = {
      val out = new Output(outStream)
      kryo.get.writeClassAndObject(out,value)
    }

    def decode(inStream: InputStream): A = {
      val in = new Input(inStream)
      return kryo.get.readClassAndObject(in).asInstanceOf[A]
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .flatMap(s => s.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupByKey()
      .map { case (word, counts) => (word, counts.sum) }
      .map(kv => kv._1 + " " + kv._2)

    PAssert.that(result.internal).containsInAnyOrder(expected)

    sc.close()
  }
}
