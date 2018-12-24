package com.verizon.bda.trapezium.dal.lucene

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.util

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.util.DalUtils
import com.verizon.bda.trapezium.dal.lucene.LuceneDAO.{PREFIX, SUFFIX, INDEX_PREFIX}


/**
  * Created by sankma8 on 6/20/18.
  */
case class LuceneReader(leafReader: LeafReader, range: FeatureAttr)


class LuceneShard(hdfsPath: String,
                  indexPath: String,
                  preload: Boolean,
                  converter: OLAPConverter,
                  analyzerStr: String) extends ILuceneIndex with Serializable {

  @transient
  var luceneShard : LocalLuceneShard = createLocalLuceneShard(indexPath,
    preload,
    converter,
    analyzerStr)


  private def createLocalLuceneShard(indexPath: String, preload: Boolean, converter: OLAPConverter,
                                     analyzerStr: String) : LocalLuceneShard = {
    val directory = new MMapDirectory(new File(indexPath).toPath)
    directory.setPreload(preload)
    val reader = DirectoryReader.open(directory)
    val analyzer: Analyzer = LuceneDAO.getAnalyzer(analyzerStr)
    new LocalLuceneShard(reader, converter, analyzer)
  }


  override def searchDocs(queryStr: String, sample: Double): util.BitSet = {
    if (luceneShard == null){
      LuceneShard.getLogger.info(s"Search indexPath:${new File(indexPath).exists()}:${hdfsPath}:${indexPath}")
      if(!new File(indexPath).exists()) {
        LuceneShard.getLogger.info(s"Search LuceneShard construction: Copying the hdfs file to local from ${hdfsPath}")
        LuceneShard.copyToLocal(hdfsPath, indexPath.toString)
      }
      /* else {
        LuceneShard.getLogger.info(s"Search LuceneShard construction: indexPath exists ${hdfsPath}")
        val sparkConf = new SparkConf()
        val s1 = hdfsPath.substring(hdfsPath.indexOf(SUFFIX))
        val s2 = s1.substring(0, s1.indexOf("/"))
        val index = s2.substring(s2.indexOf("-") + 1)
        val localDir = new File(DalUtils.getLocalDir(sparkConf) + "/" + SUFFIX + index)
        // Open a directory on Standalone/YARN/Mesos disk cache
        val shuffleIndexFile = DalUtils.getTempFile(PREFIX, localDir)
        indexPath = shuffleIndexFile.toPath.toString

        LuceneShard.getLogger.info(s"Search LuceneShard construction: copy to local ${localDir}")
        LuceneShard.copyToLocal(hdfsPath, indexPath.toString)
      } */
      luceneShard = createLocalLuceneShard(indexPath, preload, converter, analyzerStr)
      LuceneShard.getLogger.info(s"Search Created the lucene shard ${hdfsPath} -> ${luceneShard}")
    }
    luceneShard.searchDocs(queryStr, sample)
  }

  override def searchDocsWithRelevance(queryStr: String, sample: Double): Array[ScoreDoc] = {
    luceneShard.searchDocsWithRelevance(queryStr, sample)
  }

  override def search(queryStr: String, columns: Seq[String], sample: Double): Iterator[Row] = {
    luceneShard.search(queryStr, columns, sample)
  }

  override def filter(docs: util.BitSet, column: String, min: Long, max: Long): util.BitSet = {
    luceneShard.filter(docs, column, min, max)
  }

  override def aggregate(queryStr: String, measure: String, agg: OLAPAggregator): OLAPAggregator = {
    luceneShard.aggregate(queryStr, measure, agg)
  }

  override def facet(queryStr: String, dimension: String, dimOffset: Int, agg: OLAPAggregator): OLAPAggregator = {
    luceneShard.facet(queryStr , dimension , dimOffset , agg)
  }

  override def group(queryStr: String, dimension: String, dimOffset: Int, measure: String,
                     minTime: Long, maxTime: Long, agg: OLAPAggregator): OLAPAggregator = {
    luceneShard.group(queryStr, dimension, dimOffset, measure, minTime, maxTime, agg)
  }

  override def timeseries(queryStr: String, minTime: Long, maxTime: Long, rollup: Long, measure: String,
                          agg: OLAPAggregator): OLAPAggregator = {
    luceneShard.timeseries(queryStr, minTime, maxTime, rollup, measure, agg)
  }

  def count(query: String): Long = {
    luceneShard.count(luceneShard.parseQuery(query))
  }

  def parseQuery(queryStr: String): Query = {
    luceneShard.parseQuery(queryStr)
  }

  def getIndexReader(): IndexReader = {
    luceneShard.getIndexReader
  }

  def search(query: String, n: Int) : TopDocs = {
    luceneShard.search(parseQuery(query), n)
  }


  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }


  /** Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()

    /**
      * This condition happens when Spark moves the executor to a different node.
      * We have to now copy the index from HDFS back to the expected location
      * before creating the LuceneShard from that location
      */
    if(!new File(indexPath).exists()) {
      LuceneShard.getLogger.info(s"LuceneShard construction: Copying the hdfs file to local from ${hdfsPath}")
      LuceneShard.copyToLocal(hdfsPath, indexPath.toString)
    }

    LuceneShard.getLogger.info("Creating the lucene shard")
    luceneShard = createLocalLuceneShard(indexPath, preload, converter, analyzerStr)
  }


}

object LuceneShard {

  @transient var log : Logger = null

  def getLogger : Logger = {
    if (log == null) log = Logger.getLogger( classOf[LuceneShard] )
    log
  }

  def apply(hdfsPath: String,
            path: String,
            preload: Boolean,
            converter: OLAPConverter,
            analyzer: String): LuceneShard = {
    new LuceneShard(hdfsPath, path, preload, converter, analyzer)
  }

  def copyToLocal(hdfsPath: String, indexPath: String): Unit = {
    val conf = new org.apache.hadoop.conf.Configuration
    FileSystem.get(conf).copyToLocalFile(false,
      new HadoopPath(hdfsPath),
      new HadoopPath(indexPath))
    getLogger.info(s"Copied the hdfs file to local from ${hdfsPath}")
  }
}
