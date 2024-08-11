package scripts;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    
    private static final String CONTENT_PREFIX = "content_";
    private static final String SUMMARY_PREFIX = "summary_";

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Adjust column indices based on your dataset
        String[] columns = line.split(",", -1);

        // Handle 'content' column (index 2)
        if (columns.length > 2) {
            String content = columns[2];
            processColumn(CONTENT_PREFIX, content, context);
        }

        // Handle 'summary' column (index 3)
        if (columns.length > 3) {
            String summary = columns[3];
            processColumn(SUMMARY_PREFIX, summary, context);
        }
    }
    
    private void processColumn(String prefix, String columnText, Context context) throws IOException, InterruptedException {
        String[] words = columnText.split("\\s+");
        for (String wordText : words) {
            String cleanedWord = wordText.replaceAll("\\W", "").toLowerCase(); // Remove punctuation and lowercase
            if (!cleanedWord.isEmpty()) {
                word.set(prefix + cleanedWord); // Add prefix to differentiate columns
                context.write(word, ONE);
            }
        }
    }
}
