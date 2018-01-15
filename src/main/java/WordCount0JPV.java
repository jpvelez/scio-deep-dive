import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

        p
                .apply(Create.of(input))
                .apply(ParDo.of(new WordParser()))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String word = c.element();
                        if (!word.isEmpty()) {
                            c.output(word);
                        }
                    }
                }))
                .apply(Count.perElement())
                .apply(ParDo.of(new DoFn<KV<String,Long>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> kv = c.element();
                        String word = kv.getKey();
                        Long count = kv.getValue();
                        c.output(word + " " + count);
                    }
                }))
                .apply(TextIO.write().to("output.txt"));

        p.run();
    }

    public static class WordParser extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            for (String word : line.toLowerCase().split(" ")) {
                c.output(word);
            }
        }
    }
}
