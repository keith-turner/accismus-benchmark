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
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.accismus.api.Admin;
import org.apache.accumulo.accismus.api.Column;
import org.apache.accumulo.accismus.api.ColumnIterator;
import org.apache.accumulo.accismus.api.LoaderExecutor;
import org.apache.accumulo.accismus.api.RowIterator;
import org.apache.accumulo.accismus.api.ScannerConfiguration;
import org.apache.accumulo.accismus.api.Snapshot;
import org.apache.accumulo.accismus.api.SnapshotFactory;
import org.apache.accumulo.accismus.api.config.AccismusProperties;
import org.apache.accumulo.accismus.api.config.InitializationProperties;
import org.apache.accumulo.accismus.api.config.LoaderExecutorProperties;
import org.apache.accumulo.accismus.api.test.MiniAccismus;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class BenchTestIT {
  
  // TODO use code from Accismus

  protected static String secret = "ITSecret";
  
  protected static ZooKeeper zk;
  
  protected static final Map<Column,String> EMPTY_OBSERVERS = new HashMap<Column,String>();
  
  protected static AtomicInteger next = new AtomicInteger();
  
  private static Instance instance;
  
  protected Connector conn;
  protected String table;
  protected String zkn;
  
  protected AccismusProperties connectionProps;

  private MiniAccismus miniAccismus;

  @BeforeClass
  public static void setUp() throws Exception {
    String instanceName = "plugin-it-instance";
    instance = new MiniAccumuloInstance(instanceName, new File("target/accumulo-maven-plugin/" + instanceName));
    zk = new ZooKeeper(instance.getZooKeepers(), 30000, null);
  }
  
  @Before
  public void setup() throws Exception {
    
    conn = instance.getConnector("root", new PasswordToken(secret));
    
    table = "table" + next.getAndIncrement();
    zkn = "/test" + next.getAndIncrement();
    
    connectionProps = new InitializationProperties().setAccumuloTable(table).setObservers(getObservers()).setNumThreads(1)
        .setZookeepers(instance.getZooKeepers()).setZookeeperRoot(zkn).setAccumuloInstance(instance.getInstanceName()).setAccumuloUser("root")
        .setAccumuloPassword(secret);

    Admin.initialize(connectionProps);

    miniAccismus = new MiniAccismus(connectionProps);
    miniAccismus.start();

  }
  
  @After
  public void tearDown() throws Exception {
    miniAccismus.stop();
    conn.tableOperations().delete(table);
  }
  
  protected Map<Column,String> getObservers() {
    Map<Column,String> colObservers = new HashMap<Column,String>();
    colObservers.put(Generator.contetCol, ClusterIndexer.class.getName());
    return colObservers;
  }

  @Test
  public void test1() throws Exception {
    
    LoaderExecutorProperties lep = new LoaderExecutorProperties(connectionProps);
    lep.setNumThreads(0).setQueueSize(0);
    LoaderExecutor lexecutor = new LoaderExecutor(lep);

    Map<ByteSequence,Document> expected = new HashMap<ByteSequence,Document>();

    Random rand = new Random();
    
    for (int i = 0; i < 10; i++) {
      Document doc = new Document(rand);
      expected.put(doc.getUrl(), doc);
      lexecutor.execute(new DocumentLoader(doc));
    }
    
    miniAccismus.waitForObservers();

    verify(expected);
    verifyMR();
    
    // update a document
    ByteSequence uri = expected.keySet().iterator().next();
    Random r = new Random();
    byte newContent[] = new byte[1004];
    r.nextBytes(newContent);
    Document newDoc = new Document(uri, new ArrayByteSequence(newContent));
    
    lexecutor.execute(new DocumentLoader(newDoc));

    expected.put(uri, newDoc);

    miniAccismus.waitForObservers();
    
    verify(expected);
    
    miniAccismus.waitForObservers();
  }

  private void verifyMR() throws Exception {
    
    // TODO use junit tmp file
    String propsFile = FileUtils.getTempDirectoryPath() + File.separator + "ab_verify" + UUID.randomUUID().toString() + ".props";
    String outputDir = FileUtils.getTempDirectoryPath() + File.separator + "ab_verify" + UUID.randomUUID().toString();
    
    FileWriter fw = new FileWriter(propsFile);
    connectionProps.store(fw, "");
    fw.close();
    
    Verifier.main(new String[] {"-D", "mapred.job.tracker=local", "-D", "fs.default.name=file:///", propsFile, outputDir});
  }

  /**
   * @param expected
   * 
   */
  private void verify(Map<ByteSequence,Document> expected) throws Exception {
    Snapshot tx1 = new SnapshotFactory(connectionProps).createSnapshot();
    
    RowIterator riter = tx1.get(new ScannerConfiguration());
    
    HashSet<ByteSequence> docsSeen = new HashSet<ByteSequence>();

    HashSet<ByteSequence> docsSeenK1 = new HashSet<ByteSequence>();
    HashSet<ByteSequence> docsSeenK2 = new HashSet<ByteSequence>();
    HashSet<ByteSequence> docsSeenK3 = new HashSet<ByteSequence>();

    while (riter.hasNext()) {
      Entry<ByteSequence,ColumnIterator> cols = riter.next();
      String row = cols.getKey().toString();
      
      if (row.startsWith("ke")) {
        ColumnIterator citer = cols.getValue();
        while (citer.hasNext()) {
          Entry<Column,ByteSequence> cv = citer.next();
          
          Document doc = expected.get(cv.getKey().getQualifier());
          
          ByteSequence ek = null;
          if (row.startsWith("ke1")) {
            ek = doc.getKey1();
            docsSeenK1.add(cv.getKey().getQualifier());
          } else if (row.startsWith("ke2")) {
            ek = doc.getKey2();
            docsSeenK2.add(cv.getKey().getQualifier());
          } else if (row.startsWith("ke3")) {
            ek = doc.getKey3();
            docsSeenK3.add(cv.getKey().getQualifier());
          }
          
          Assert.assertEquals(ek, cols.getKey());

        }
      } else {
        Assert.assertTrue(docsSeen.add(cols.getKey()));
      }
    }

    Assert.assertEquals(expected.keySet(), docsSeen);
    
    Assert.assertEquals(expected.keySet(), docsSeenK1);
    Assert.assertEquals(expected.keySet(), docsSeenK2);
    Assert.assertEquals(expected.keySet(), docsSeenK3);
  }
  
  // TODO test deleting document
}
