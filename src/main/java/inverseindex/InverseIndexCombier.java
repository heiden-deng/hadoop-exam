package inverseindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InverseIndexCombier extends Reducer<Text,Text,Text,Text>{
    private Text k = new Text();
    private Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] words = key.toString().split("->");
        String word = words[0];
        String path = words[1];
        int counter = 0;
        for (Text t : values){
            counter += Integer.parseInt(t.toString());
        }

        k.set(word);
        v.set(path + "->" + counter);
        context.write(k,v);

    }
}
