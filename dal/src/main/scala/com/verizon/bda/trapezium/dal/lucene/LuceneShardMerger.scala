package com.verizon.bda.trapezium.dal.lucene

import com.verizon.bda.trapezium.dal.solr.CollectIndices
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkContext
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, LogMergePolicy, TieredMergePolicy}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.store.Directory
import org.apache.solr.store.hdfs.HdfsDirectory

import collection.JavaConverters._

@SerialVersionUID(6732270565076291202L)
object LuceneShardMerger extends Serializable {
//  def merge(sc: SparkContext,
//            outputPath: String,
//            numShards: Int) : Unit = {
//    // 1. Read the part files from location/INDICES_PREFIX and then
// get all the names of the part files
//    val indexPath = location.stripSuffix("/") + "/" + INDICES_PREFIX
//    val indexDir = new HadoopPath(indexPath)
//    val fs = FileSystem.get(indexDir.toUri, sc.hadoopConfiguration)
//
//    val status: Array[FileStatus] = fs.listStatus(indexDir, new PathFilter {
//      override def accept(path: HadoopPath): Boolean = {
//        path.getName.startsWith(SUFFIX)
//      }
//    })
//    //2. Distribute the part names within rdd with partition as numShard
//    //3. Use indexMerge API
//    //4. Output to outputPath/INDICES_PREFIX
//    sc.parallelize(status, numShards).mapPartitionsWithIndex((partition, files) => {
//
//      val mergePath = outputPath + s"part-${partition}"
//
//      var conf = new Configuration()
//
//      val mergedIndex = new HdfsDirectory(new HadoopPath(mergePath), conf)
//
//      val indexWriterConfig = new IndexWriterConfig()
//      indexWriterConfig.setOpenMode(OpenMode.CREATE).setUseCompoundFile(false)
//
//
//      val writer = new IndexWriter(mergedIndex, indexWriterConfig)
//      val indexes = new Array[Directory](files.size)
//      var counter = 0
//
//      while (files.hasNext) {
//        val file = files.next()
//        // Construct name with locationPath + file.getPath
//        // Use IndexMerge API to generate a single index file
//        indexes(counter) = new HdfsDirectory(new HadoopPath(file.getPath.getName), conf)
//        counter = counter+1
//      }
//
//      log.info("Logically merging " + files.size + " shards into one shard")
//      val start = System.currentTimeMillis
//
//      //perform the actual merge
//      writer.addIndexes(indexes: _*)
//
//      val secs = (System.currentTimeMillis() - start) / 1000.0f
//      log.info(s"Logical merge took {$secs} secs")
//
//      ???
//
//    })
//  }
  def mergeShardsWithSpark(sc: SparkContext, finalIndexCount: Int, indexPath: String,
                           nameNode: String, folderPrefix: String, finalPath: String): Unit = {
    val hdfsFilesSeq: Seq[String] = CollectIndices.getHdfsList(nameNode,
      folderPrefix, indexPath).toSeq
    sc.parallelize(0 to hdfsFilesSeq.size).map(p => List(p)).repartition(finalIndexCount)
      .mapPartitionsWithIndex((id, p) => {
        p.toList.map(p => (id, p)).iterator
      }).reduceByKey(_ ++ _).map(p => {
      val paths = p._2.map(p => new Path("hdfs://" + nameNode + indexPath + p))
      val index = p._1
      val mergedShardPath = new Path(finalPath + folderPrefix + index + "/index")
      mergeShards(paths, mergedShardPath)
    }).collect()

  }

  def mergeShards(currentIndexDirs: List[Path], mergedShardPath: Path): Unit = {

    // Adopted from TreeMergeOutputFormat.java of Cloudera Search
    // merges the input offline shards into a single shard
    // create a shard directory under mergedShardPath


    try {
      val conf: Configuration = new Configuration()
      conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)

      val mergedIndex = new HdfsDirectory(mergedShardPath, conf)
      val writerConfig = new IndexWriterConfig()
        .setOpenMode(OpenMode.CREATE).setUseCompoundFile(false)

      val mergePolicy = writerConfig.getMergePolicy

      mergePolicy match {
        case p: TieredMergePolicy => mergePolicy.setNoCFSRatio(0.0)
        case p: LogMergePolicy => mergePolicy.setNoCFSRatio(0.0)
      }

      val writer = new IndexWriter(mergedIndex, writerConfig)

      val indexes = new Array[Directory](currentIndexDirs.size)
      for (x <- 0 to (currentIndexDirs.size - 1)) {
        indexes(x) = new HdfsDirectory((new Path(currentIndexDirs(x), "/  index")), conf)
      }
      writer.addIndexes(indexes: _*)
      writer.commit()
      writer.close
    } // end try
    catch {
      case e: Exception => // log.warn("Caught exception while merging shards: ", e)
    }
  } // end mergeShards


}
