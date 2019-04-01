package inverseindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class InverseIndexReducer extends Reducer<Text,Text,Text,Text>{

    //private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder resultBuilder = new StringBuilder();
        for (Text t : values){
            resultBuilder.append((t.toString() + ",").toCharArray());
        }
        v.set(resultBuilder.toString().trim());
        context.write(key,v);
    }
}
