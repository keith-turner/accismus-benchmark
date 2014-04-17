/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package accismus.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import accismus.api.Column;
import accismus.api.ColumnIterator;
import accismus.api.config.AccismusProperties;
import accismus.api.mapreduce.AccismusInputFormat;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * map redice job that can verify index is consistent
 */
public class Verifier extends Configured implements Tool {
  
  private static ByteSequence DUP = new ArrayByteSequence("dup");
  private static ByteSequence KEY = new ArrayByteSequence("key");
  
  private static Text D = new Text("d");
  private static Text K = new Text("k");

  public static class VMapper extends Mapper<ByteSequence,ColumnIterator,Text,Text> {
    
    private Text okey = new Text();

    public void map(ByteSequence row, ColumnIterator columns, Context context) throws IOException, InterruptedException {
      while (columns.hasNext()) {
        
        ByteSequence url;
        ByteSequence key;
        Text val;

        Entry<Column,ByteSequence> col = columns.next();
        if (col.getKey().getFamily().equals(DUP)) {
          url = col.getKey().getQualifier();
          key = row;
          val = D;
        } else if (col.getKey().getFamily().equals(KEY)) {
          url = row;
          key = col.getValue();
          val = K;
        } else {
          throw new IllegalArgumentException();
        }
        
        okey.set(url.getBackingArray(), url.offset(), url.length());
        okey.append(key.getBackingArray(), key.offset(), key.length());

        context.write(okey, val);
      }
    }
  }
  
  static enum Status {
    GOOD, BAD
  }
  
  public static class VReducer extends Reducer<Text,Text,Text,Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int docCount = 0;
      int indexCount = 0;
      
      for (Text text : values) {
        if (text.equals(D)) {
          docCount++;
        } else if (text.equals(K)) {
          indexCount++;
        } else {
          throw new IllegalArgumentException();
        }
      }
      
      if (docCount == 1 && indexCount == 1) {
        context.getCounter(Status.GOOD).increment(1);
      } else {
        context.getCounter(Status.BAD).increment(1);
        context.write(key, new Text());
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 2) {
      System.err.println("Usage : " + this.getClass().getSimpleName() + " <props file> <output dir>");
      return 1;
    }

    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    
    AccismusProperties accisumusProps = new AccismusProperties(new File(args[0]));
    
    AccismusInputFormat.configure(job, accisumusProps);
    AccismusInputFormat.fetchFamilies(job, KEY, DUP);

    job.setInputFormatClass(AccismusInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setMapperClass(VMapper.class);
    
    job.setReducerClass(VReducer.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    
    TextOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Verifier(), args);
    if (res != 0)
      System.exit(res);
  }
}
