import WordCount0JPV.{SimpleDoFn, expected, input}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create.Values
import org.apache.beam.sdk.transforms.{Count, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

object WordCount1JPV {

  object ScioContext {
    def apply(args: Array[String]): ScioContext = new ScioContext(args)
  }

  class ScioContext(args: Array[String]) {
    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)
    def parallelize[A](input: Iterable[A]): PCollection[A] = {
      pipeline.apply(new Values[A](input))
    }
    def close() = pipeline.run()
  }


  def main(args: Array[String]): Unit = {
    val sc = ScioContext(args)

    val result: PCollection[String] = sc.parallelize(input)
      .apply(ParDo.of(new SimpleDoFn[String, String]("flatMap") {
        override def process(c: ProcessContext) {
          val line = c.element()
          for (word <- line.toLowerCase().split(" ")) {
            c.output(word)
          }
        }
      }))
      .apply(ParDo.of(new SimpleDoFn[String, String]("filter") {
        override def process(c: ProcessContext) {
          val word = c.element()
          if (!word.isEmpty()) {
            c.output(word)
          }
        }
      }))
      .apply(Count.perElement())
      .apply(ParDo.of(new SimpleDoFn[KV[String,Long], String]("map") {
        override def process(c: ProcessContext) {
          val kv: KV[String, Long] = c.element()
          val word = kv.getKey()
          val count = kv.getValue()
          c.output(word + " " + count)
        }
      })

    PAssert.that(result).containsInAnyOrder(expected)

    sc.close()
  }

}
