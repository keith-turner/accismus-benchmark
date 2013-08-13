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
package org.apache.accumulo.accismus.benchmark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.accismus.api.Column;
import org.apache.accumulo.accismus.api.Loader;
import org.apache.accumulo.accismus.api.mapreduce.AccismusOutputFormat;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */
public class Generator extends Configured implements Tool {
  
  public static class DocumentInputSplit extends InputSplit implements Writable {
    
    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }
    
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }
    
    @Override
    public void readFields(DataInput arg0) throws IOException {}
    
    @Override
    public void write(DataOutput arg0) throws IOException {}
  }
  
  public static class DocumentInputFormat extends InputFormat<ByteSequence,Document> {
    
    @Override
    public RecordReader<ByteSequence,Document> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
      return new RecordReader<ByteSequence,Document>() {
        
        private int numDocs;
        private int docNum;
        private Random rand;
        private Document currDoc;
        
        @Override
        public void close() throws IOException {
        }
        
        @Override
        public ByteSequence getCurrentKey() throws IOException, InterruptedException {
          return currDoc.getUrl();
        }
        
        @Override
        public Document getCurrentValue() throws IOException, InterruptedException {
          return currDoc;
        }
        
        @Override
        public float getProgress() throws IOException, InterruptedException {
          return (float) docNum / (float) numDocs;
        }
        
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
          this.numDocs = context.getConfiguration().getInt(DocumentInputFormat.class.getName() + ".numDocuments", 100);
          this.docNum = 0;
          this.rand = new Random();
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (docNum >= numDocs)
            return false;
          this.currDoc = new Document(rand);
          docNum++;
          return true;
        }
      };
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      int numSplits = context.getConfiguration().getInt(DocumentInputFormat.class.getName() + ".numSplits", 1);
      
      ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
      for (int i = 0; i < numSplits; i++) {
        splits.add(new DocumentInputSplit());
      }
      
      return splits;
    }
    
    public static void setNumTask(Job job, int numSplits, int numDocuments) {
      job.getConfiguration().setInt(DocumentInputFormat.class.getName() + ".numSplits", numSplits);
      job.getConfiguration().setInt(DocumentInputFormat.class.getName() + ".numDocuments", numDocuments / numSplits);
    }
    
  }

  public static final Column contetCol = new Column("doc", "content");
  
  public static class GMapper extends Mapper<ByteSequence,Document,Loader,NullWritable> {
    
    public void map(ByteSequence uri, Document doc, Context context) throws IOException, InterruptedException {
      context.write(new DocumentLoader(doc), NullWritable.get());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 3) {
      System.err.println("Usage : " + this.getClass().getSimpleName() + " <props file> <num task> <num documents>");
      return 1;
    }

    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());

    
    job.setInputFormatClass(DocumentInputFormat.class);
    
    DocumentInputFormat.setNumTask(job, Integer.parseInt(args[1]), Integer.parseInt(args[2]));

    job.setMapOutputKeyClass(Loader.class);
    job.setMapOutputValueClass(NullWritable.class);
    
    job.setMapperClass(GMapper.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccismusOutputFormat.class);
    
    Properties accisumusProps = new Properties();
    accisumusProps.load(new FileReader(args[0]));
    AccismusOutputFormat.configure(job, accisumusProps);
    
    job.waitForCompletion(true);

    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Generator(), args);
  }
}
