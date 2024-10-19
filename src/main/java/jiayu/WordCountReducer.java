package jiayu;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, Text> countToWordMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        // 将计数作为键，单词作为值
        // 使用 TreeMap 保持自动排序
        countToWordMap.put(sum, new Text(key));

        // 保持 TreeMap 只包含前100个高频单词
        if (countToWordMap.size() > 100) {
            countToWordMap.remove(countToWordMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 由于 TreeMap 按键升序排列，逆序遍历以获得降序结果
        for (Map.Entry<Long, Text> entry : countToWordMap.descendingMap().entrySet()) {
            context.write(entry.getValue(), new LongWritable(entry.getKey()));
        }
    }
}
