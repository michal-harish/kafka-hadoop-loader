package net.imagini.kafka.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;

public class KafkaOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException
    {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null)
        {
            throw new InvalidJobConfException("Output directory not set.");
        }

        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(
            job.getCredentials(), 
            new Path[] {outDir}, 
            job.getConfiguration()
        );
    }

    /**
     * Create a composite record writer that can write key/value data to different
     * output files
     * 
     * @param fs
     *          the file system to use
     * @param job
     *          the job conf for the job
     * @param name
     *          the leaf file name for the output file (such as part-00000")
     * @param arg3
     *          a progressable for reporting progress.
     * @return a composite record writer
     * @throws IOException
     */
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

      final TaskAttemptContext taskContext = context;
      final Configuration conf = context.getConfiguration();
      final boolean isCompressed = getCompressOutput(context);
      String ext = "";
      CompressionCodec gzipCodec = null;
      if (isCompressed) {
          Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
          gzipCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
          ext = ".gz";
      }
      final CompressionCodec codec = gzipCodec;
      final String extension = ext;

      return new RecordWriter<K, V>() {
        TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();
        public void write(K key, V value) throws IOException {

          String keyBasedPath = "d="+key.toString();

          RecordWriter<K, V> rw = this.recordWriters.get(keyBasedPath);
          try {
              if (rw == null) {
                  Path file = new Path(
                        ((FileOutputCommitter)getOutputCommitter(taskContext)).getWorkPath(), 
                        getUniqueFile(
                            taskContext, keyBasedPath + "/" + taskContext.getJobID().toString().replace("job_", ""), 
                            extension
                        )
                  );
                  FileSystem fs = file.getFileSystem(conf);
                  FSDataOutputStream fileOut = fs.create(file, false);
                  if (isCompressed)
                  {
                      rw = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
                  }
                  else
                  {
                      rw = new LineRecordWriter<K, V>(fileOut);
                  }
                  this.recordWriters.put(keyBasedPath, rw);
              }
              rw.write( key, value);
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
        };

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException 
        {
            Iterator<String> keys = this.recordWriters.keySet().iterator();
            while (keys.hasNext()) {
                RecordWriter<K, V> rw = this.recordWriters.get(keys.next());
                rw.close(context);
              }
              this.recordWriters.clear();
        };
      };
    }

    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> 
    {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        static {
          try {
            newline = "\n".getBytes(utf8);
          } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
          }
        }

        protected DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
          this.out = out;
        }

        /**
         * Write the event value to the byte stream.
         * 
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
          if (o instanceof Text) {
            Text to = (Text) o;
            out.write(to.getBytes(), 0, to.getLength());
          } else {
            out.write(o.toString().getBytes(utf8));
          }
        }

        public synchronized void write(K key, V value)
          throws IOException {

          boolean nullValue = value == null || value instanceof NullWritable;
          if (nullValue) {
            return;
          }
          writeObject(value);
          out.write(newline);
        }

        public synchronized 
        void close(TaskAttemptContext context) throws IOException {
          out.close();
        }
    }
}
