/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.Seq$;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMORReadOptimized extends SparkClientFunctionalTestHarness {
  private HoodieTableMetaClient metaClient;
  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  void setUp() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().toString());
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, properties);
    dataGen = new HoodieTestDataGenerator();
  }

  @Test
  public void testGetBaseFileAfterScheduleCompaction() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder(true).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      metaClient = HoodieTableMetaClient.reload(metaClient);

      /**
       * Write 2 (only upsert, written as log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, 20);

      statuses = client.upsert(jsc().parallelize(newRecords), newCommitTime).collect();
      assertNoWriteErrors(statuses);
      metaClient = HoodieTableMetaClient.reload(metaClient);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);

      /**
       * Get partition with base file (only one file-group and one file-slice in partition)
       */
      Set<String> baseFileListBeforeCompaction = new HashSet<>();

      for (String partitionPath : dataGen.getPartitionPaths()) {

        List<FileSlice> groupedLogFiles =
            hoodieTable.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());

        assertTrue(groupedLogFiles.size() == 1);
        assertTrue(groupedLogFiles.get(0).getBaseFile().isPresent());

        baseFileListBeforeCompaction.add(groupedLogFiles.get(0).getBaseFile().get().getFileName());
      }

      /***
       * schedule compaction
       */
      client.scheduleCompaction(Option.empty());
      metaClient = HoodieTableMetaClient.reload(metaClient);

      /***
       * get read_optimized base file view
       */
      Map<String, String> optParamMap = new HashMap<String, String>();
      optParamMap.put("hoodie.datasource.query.type", DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL());
      optParamMap.put("path", basePath());

      List<Tuple2<String, String>> optParamTuples = optParamMap.entrySet().stream()
          .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
          .collect(Collectors.toList());

      scala.collection.immutable.Map optParams = scala.collection.immutable.Map$.MODULE$.apply(JavaConversions.asScalaBuffer(optParamTuples).toSeq());

      BaseFileOnlyRelation baseFileOnlyRelation = new BaseFileOnlyRelation(sqlContext(), metaClient, optParams, scala.Option.empty(), (Seq<Path>)Seq$.MODULE$.empty());
      List<FileStatus> fileStatusSeq = scala.collection.JavaConversions.seqAsJavaList(baseFileOnlyRelation.fileIndex().allFiles());

      Set<String> baseFileReadAfterCompaction = fileStatusSeq.stream()
              .map(fileStatus -> fileStatus.getPath().getName())
          .collect(Collectors.toSet());

      assertTrue(baseFileListBeforeCompaction.size() == baseFileReadAfterCompaction.size());
      assertTrue(baseFileListBeforeCompaction.equals(baseFileReadAfterCompaction));
    }
  }
}
