import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

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

        p.apply(Create.of(input))
                .apply(ParDo.of(new WordParser()))
                .apply(Combine.perKey(Sum.ofIntegers()))
                .apply(ParDo.of(new DoFn<KV<String,Integer>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String out = c.element().getKey() + " " + c.element().getValue();
                        c.output(out);
                    }
                }))
                .apply(TextIO.write().to("output.txt"));

        p.run();
    }

    public static class WordParser extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(" ");
            for (int i = 0; i < words.length; i++) {
                KV<String, Integer> pair = KV.of(words[i].toLowerCase(), 1);
                c.output(pair);
            }
        }
    }
}
