package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text,Text,Text,Text>{

    private List<Text> listA = new ArrayList<Text>();
    private List<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();
        for (Text text: values){
            if (text.charAt(0) == 'A'){
                listA.add(new Text(text.toString().substring(1)));
            }else{
                listB.add(new Text(text.toString().substring(1)));
            }
        }
        executeJoinLogic(context);

    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if (joinType.equalsIgnoreCase("inner")){
            if (!listA.isEmpty() && !listB.isEmpty()){
                for (Text ta: listA){
                    for (Text tb: listB){
                        context.write(ta,tb);
                    }
                }
            }
        }else if (joinType.equalsIgnoreCase("leftouter")){
            for (Text ta: listA){
                if (!listB.isEmpty()){
                    for (Text tb: listB){
                        context.write(ta,tb);
                    }
                }else{
                    context.write(ta,new Text(""));
                }
            }
        }else if (joinType.equalsIgnoreCase("rightouter")){
            for (Text tb: listB){
                if (!listA.isEmpty()){
                    for (Text ta: listA){
                        context.write(ta,tb);
                    }
                }else{
                    context.write(new Text(""),tb);
                }
            }
        }else if (joinType.equalsIgnoreCase("fullouter")){
            if (!listA.isEmpty()){
                for (Text ta: listA){
                    if (!listB.isEmpty()){
                        for (Text tb: listB){
                            context.write(ta, tb);
                        }
                    }else{
                        context.write(ta, new Text(""));
                    }
                }
            }else{
                for (Text tb: listB){
                    context.write(new Text(""), tb);
                }
            }
        }else{
            throw new RuntimeException("Join连接设置成：inner,leftouter,rightouter,fullouter");
        }
    }
}
