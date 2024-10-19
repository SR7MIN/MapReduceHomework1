package jiayu;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, Text> countToStockMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }

        // 将计数作为键，股票代码作为值
        // 使用 TreeMap 保持自动排序
        countToStockMap.put(sum, new Text(key));

        // 保持 TreeMap 只包含前100个高频股票
        // if (countToStockMap.size() > 100) {
        //     countToStockMap.remove(countToStockMap.firstKey());
        // }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 由于 TreeMap 按键升序排列，逆序遍历以获得降序结果
        for (Map.Entry<Long, Text> entry : countToStockMap.descendingMap().entrySet()) {
            context.write(entry.getValue(), new LongWritable(entry.getKey()));
        }
    }
}