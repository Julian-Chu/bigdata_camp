import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MobileMapReduce {

    public static class MobileMapper
            extends Mapper<LongWritable, Text, Text, Mobile> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            String phone = fields[1];
            long upstream = Long.parseLong(fields[7]);
            long downstream = Long.parseLong(fields[8]);

            context.write(new Text(phone), new Mobile(phone, upstream, downstream));
        }
    }

    public static class MobileReducer
            extends Reducer<Text, Mobile, Text, Mobile> {

        public void reduce(Text key, Iterable<Mobile> values,
                           Context context
        ) throws IOException, InterruptedException {
            long upstreamSum = 0;
            long downstreamSum = 0;

            for(Mobile m: values) {
                upstreamSum += m.getUpStream();
                downstreamSum += m.getDownStream();
            }

            context.write(key, new Mobile(key.toString(), upstreamSum, downstreamSum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mobile statistics");
        job.setJarByClass(Mobile.class);
        job.setMapperClass(MobileMapper.class);
        job.setReducerClass(MobileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mobile.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
