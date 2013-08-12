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

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.accismus.api.AccismusProperties;
import org.apache.accumulo.accismus.api.Column;
import org.apache.accumulo.accismus.api.LoaderTaskFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */
public class Generator extends Configured implements Tool {
  
  public static final Column contetCol = new Column("doc", "content");
    
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Generator(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    
    ExecutorService executor = Executors.newFixedThreadPool(10);
    LoaderTaskFactory lwrapper = new LoaderTaskFactory(new AccismusProperties(new File(args[0])));

    int num = Integer.parseInt(args[1]);
    
    Random rand = new Random();

    for (int i = 0; i < num; i++) {
      Document doc = new Document(rand);
      executor.submit(lwrapper.newLoadTask(new DocumentLoader(doc)));
    }
    
    executor.shutdown();
    while (!executor.isShutdown()) {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    return 0;
  }
}
