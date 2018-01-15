import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class WordCount0JPV {

    public static List<String> input = Lists.newArrayList(
            "Du",
            "Du hast",
            "Du hast mich",
            "Du hast mich",
            "Du hast mich gefragt",
            "Du hast mich gefragt",
            "Du hast mich gefragt und ich hab nichts gesagt");


    public static List<String> expected = Lists.newArrayList(
            "du 7",
            "hast 6",
            "mich 5",
            "gefragt 3",
            "und 1",
            "ich 1",
            "hab 1",
            "nichts 1",
            "gesagt 1" );

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> result =  p
                .apply(Create.of(input))
                .apply(ParDo.of(new SimpleDoFn<String, String>("flatMap") {
                    @Override
                    public void process(ProcessContext c) {
                        String line = c.element();
                        for (String word : line.toLowerCase().split(" ")) {
                            c.output(word);
                        }
                    }
                }))
                .apply(ParDo.of(new SimpleDoFn<String, String>("filter") {
                    @Override
                    public void process(ProcessContext c) {
                        String word = c.element();
                        if (!word.isEmpty()) {
                            c.output(word);
                        }
                    }
                }))
                .apply(Count.perElement())
                .apply(ParDo.of(new SimpleDoFn<KV<String,Long>, String>("map") {
                    @Override
                    public void process(ProcessContext c) {
                        KV<String, Long> kv = c.element();
                        String word = kv.getKey();
                        Long count = kv.getValue();
                        c.output(word + " " + count);
                    }
                }));

        PAssert.that(result).containsInAnyOrder(expected);

        p.run().waitUntilFinish();
    }

    public static abstract class SimpleDoFn<A, B> extends DoFn<A, B> {

        public String name;

        public SimpleDoFn(String name) { this.name = name; }

        public abstract void process(DoFn<A, B>.ProcessContext c);

        public void log(String step) {
            System.out.println(
                String.format("%s %s %s thread: %s", name, step, this, Thread.currentThread().getId()));
        }

        @Setup
        public void setup() { this.log("Setup"); }

        @StartBundle
        public void startBundle() { this.log("StartBundle"); }

        @ProcessElement
        public void processElement(ProcessContext c) {
            log("ProcessElement");
            process(c);
        }

        @FinishBundle
        public void finishBundle() { this.log("FinishBundle"); }

        @Teardown
        public void teardown() { this.log("Teardown"); }

    }
}
