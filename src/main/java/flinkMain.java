
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class flinkMain {

    public static class WordWithCount {
        public String word;
        public long count;
        public WordWithCount() {}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return word + " : " + count;
        }

    }

    // 这是一个基于DataStream API的 flink的典型的流处理demo
    // 后面的文章还会提到用于批处理的DataSet API 和用于查询的 Table API
    public static void main(String[] args) throws Exception{
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port"):9000;
        } catch (Exception e) {
            System.err.println("建议指定hostname和port");
            return;
        }
        // get the execution environment
        //通常getExecutionEnvironment即可，可以根据具体环境进行选择。
        // 也可以创建本地或者远程环境createLocalEnvironment()和createRemoteEnvironment(String host，int port，String和.jar文件)
        //这是流处理入口，批处理使用ExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // get input data by connecting to the socket,
        // 基于Socket的 DataStream API socketTextStream(hostName,port,delimiter,maxRetry)
        // 该函数第三个参数的意思是分隔符，第四个是获取数据的最大尝试次数，这两个参数是可选参数
        // 此外，还有基于file的DataStream API readTextFile(String path) 基于Collection的API等，会在后面的文章中细说
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        //可以把text打印出来验证后面的计算对不对
        text.print();
        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        //利用正则表达式以一个以上空格进行划分
                        for (String word : value.split("\\s+")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }});
        // print the results with a single thread, rather than in parallel
        //并行度，后续文章会详细解释 slot，slot与Parallelism的关系
        windowCounts.print().setParallelism(1);
        //flink在内存管理分配方案上，Memory Manage pool部分默认是懒加载，需显示调用执行
        env.execute("Socket Window WordCount");
    }
}

