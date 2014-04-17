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

import accismus.api.Loader;
import accismus.api.Transaction;

/**
 * 
 */
public class DocumentLoader implements Loader {
  
  private Document doc;

  DocumentLoader(Document doc) {
    this.doc = doc;
  }
  
  @Override
  public void load(Transaction tx) throws Exception {
    // TODO see if document is different?
    tx.set(doc.getUrl(), Generator.contetCol, doc.getContent());
  }
  
}
