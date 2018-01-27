import WordCount0.{SimpleDoFn, expected, input}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.{Count, DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.collection.JavaConverters._

object WordCount2JPV {

  class ScioContext(args: Array[String]) {
    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)
    def close() = pipeline.run()

    def parallelize[A](input: Iterable[A]): SCollection[A] = {
      val col = pipeline.apply(Create.of(input.asJava))
      new SCollection[A](col)
    }
  }

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }

  class SCollection[A](val p: PCollection[A]) {
    def flatMap[B](doFn: (A => Iterable[B])): SCollection[B] = {
      val col = p.apply(ParDo.of(new SimpleDoFn[A, B]("flatMap") {
        override def process(c: DoFn[A,B]#ProcessContext) = {
          doFn(c.element()).foreach(c.output(_))
        }
        new SCollection[B](col)
      }))
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)
    val result: PCollection[String] = sc.parallelize(input.asScala)
      .flatMap(s => s.toLowerCase.split(" "))
      .flatMap(s => if (!s.isEmpty) Some(s) else None)
      .apply(Count.perElement())
      .apply(ParDo.of(new SimpleDoFn[KV[String, java.lang.Long], String]("map") {
        override def process(c: DoFn[KV[String, java.lang.Long], String]#ProcessContext) {
          val kv = c.element()
          val word = kv.getKey()
          val count = kv.getValue()
          c.output(word + " " + count)
        }
      }))

    PAssert.that(result).containsInAnyOrder(expected)

    sc.close()
  }
}
