import WordCount0.{SimpleDoFn, expected, input}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.{Count, DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object WordCount4JPV {

  class ScioContext(args: Array[String]) {
    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)
    def close() = pipeline.run()

    def parallelize[A](input: Iterable[A]): SCollection[A] = {
      val p = pipeline.apply(Create.of(input.asJava))
      new SCollection[A](p)
    }
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }


  class SCollection[A](val internal: PCollection[A]) {
    def applyTransform[B](t: PTransform[PCollection[A], PCollection[B]]) : SCollection[B] =
      new SCollection(internal.apply(t))


    def map[B: ClassTag](f: A => B): SCollection[B] = flatMap((x: A) => Some(f(x)))

    def flatMap[B: ClassTag](f: A => TraversableOnce[B]): SCollection[B] = {
      val p = internal.apply(ParDo.of(new SimpleDoFn[A, B]("flatMap") {
        override def process(c: DoFn[A,B]#ProcessContext) =
          f(c.element()).foreach(c.output)
      }))
      val cls = implicitly[ClassTag[B]].runtimeClass.asInstanceOf[Class[B]]
      val coder = internal.getPipeline.getCoderRegistry.getCoder(cls)
      p.setCoder(coder)
      new SCollection(p)
    }

  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result = sc.parallelize(input.asScala)
      .flatMap(s => s.toLowerCase.split(" "))
      .flatMap(s => if (!s.isEmpty) Some(s) else None)
      .applyTransform(Count.perElement())
      .map(kv => kv.getKey + " " + kv.getValue)

    PAssert.that(result.internal).containsInAnyOrder(expected)

    sc.close()
  }
}
