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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.accismus.Column;
import org.apache.accumulo.accismus.ColumnIterator;
import org.apache.accumulo.accismus.Configuration;
import org.apache.accumulo.accismus.Constants;
import org.apache.accumulo.accismus.Operations;
import org.apache.accumulo.accismus.RowIterator;
import org.apache.accumulo.accismus.ScannerConfiguration;
import org.apache.accumulo.accismus.Transaction;
import org.apache.accumulo.accismus.Worker;
import org.apache.accumulo.accismus.impl.ByteUtil;
import org.apache.accumulo.accismus.impl.OracleServer;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
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
  
  protected Configuration config;
  protected Connector conn;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;
  
  protected void runWorker() throws Exception, TableNotFoundException {
    Worker worker = new Worker(config);
    worker.processUpdates();
    
    // there should not be any notifcations
    Scanner scanner = conn.createScanner(table, new Authorizations());
    scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));
    
    Assert.assertFalse(scanner.iterator().hasNext());
  }
  
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
    
    Operations.initialize(conn, zkn, table, getObservers());
    config = new Configuration(zk, zkn, conn);
    
    oserver = new OracleServer(config);
    oserver.start();
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    oserver.stop();
  }
  
  protected Map<Column,String> getObservers() {
    Map<Column,String> colObservers = new HashMap<Column,String>();
    colObservers.put(Generator.contetCol, ClusterIndexer.class.getName());
    return colObservers;
  }

  @Test
  public void test1() throws Exception {
    
    Map<ByteSequence,Document> expected = new HashMap<ByteSequence,Document>();

    Random rand = new Random();
    
    for (int i = 0; i < 10; i++) {
      Document doc = new Document(rand);
      expected.put(doc.getUrl(), doc);
      Generator.insert(config, doc);
    }
    
    runWorker();

    verify(expected);
    
    // update a document
    ByteSequence uri = expected.keySet().iterator().next();
    Random r = new Random();
    byte newContent[] = new byte[1004];
    r.nextBytes(newContent);
    Document newDoc = new Document(uri, new ArrayByteSequence(newContent));
    
    Generator.insert(config, newDoc);

    expected.put(uri, newDoc);

    runWorker();
    
    verify(expected);
    
    runWorker();
  }

  /**
   * @param expected
   * 
   */
  private void verify(Map<ByteSequence,Document> expected) throws Exception {
    Transaction tx1 = new Transaction(config);
    
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
