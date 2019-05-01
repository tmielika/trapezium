package com.verizon.bda.trapezium.dal.lucene

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.util

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.log4j.Logger
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index._
import org.apache.lucene.search.{Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.MMapDirectory
import org.apache.spark.sql.Row


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
  var luceneShard: LocalLuceneShard = _

  private def createLocalLuceneShard(indexPath: String, preload: Boolean, converter: OLAPConverter,
                                     analyzerStr: String): LocalLuceneShard = {
    val directory = new MMapDirectory(new File(indexPath).toPath)
    directory.setPreload(preload)
    val reader = DirectoryReader.open(directory)
    val analyzer: Analyzer = LuceneDAO.getAnalyzer(analyzerStr)
    new LocalLuceneShard(reader, converter, analyzer)
  }


  override def searchDocs(queryStr: String, sample: Double): util.BitSet = {
    getLuceneShard.searchDocs(queryStr, sample)
  }

  private def getLuceneShard = {
    if(luceneShard==null) {
      this.synchronized{
        if(luceneShard==null) {
          if(!new File(indexPath).exists()) {
            LuceneShard.getLogger.info(s"LuceneShard construction: Copying the hdfs file to local from ${hdfsPath}")
            LuceneShard.copyToLocal(hdfsPath, indexPath.toString)
          }
          luceneShard = createLocalLuceneShard(indexPath,
            preload,
            converter,
            analyzerStr)
        }

      }
    }

    luceneShard
  }

  override def searchDocsWithRelevance(queryStr: String, sample: Double): Array[ScoreDoc] = {
    getLuceneShard.searchDocsWithRelevance(queryStr, sample)
  }

  override def search(queryStr: String, columns: Seq[String], sample: Double): Iterator[Row] = {
    getLuceneShard.search(queryStr, columns, sample)
  }

  override def filter(docs: util.BitSet, column: String, min: Long, max: Long): util.BitSet = {
    getLuceneShard.filter(docs, column, min, max)
  }

  override def aggregate(queryStr: String, measure: String, agg: OLAPAggregator): OLAPAggregator = {
    getLuceneShard.aggregate(queryStr, measure, agg)
  }

  override def facet(queryStr: String, dimension: String, dimOffset: Int, agg: OLAPAggregator): OLAPAggregator = {
    getLuceneShard.facet(queryStr, dimension, dimOffset, agg)
  }

  override def group(queryStr: String, dimension: String, dimOffset: Int, measure: String, minTime: Long, maxTime: Long, agg: OLAPAggregator): OLAPAggregator = {
    getLuceneShard.group(queryStr, dimension, dimOffset, measure, minTime, maxTime, agg)
  }

  override def timeseries(queryStr: String, minTime: Long, maxTime: Long, rollup: Long, measure: String, agg: OLAPAggregator): OLAPAggregator = {
    getLuceneShard.timeseries(queryStr, minTime, maxTime, rollup, measure, agg)
  }

  def count(query: String): Long = {
    getLuceneShard.count(getLuceneShard.parseQuery(query))
  }

  def parseQuery(queryStr: String): Query = {
    getLuceneShard.parseQuery(queryStr)
  }

  def getIndexReader(): IndexReader = {
    getLuceneShard.getIndexReader
  }

  def search(query: String, n: Int): TopDocs = {
    getLuceneShard.search(parseQuery(query), n)
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
    if (!new File(indexPath).exists()) {
      LuceneShard.getLogger.info(s"LuceneShard construction: Copying the hdfs file to local from ${hdfsPath}")
      LuceneShard.copyToLocal(hdfsPath, indexPath.toString)
    }

    LuceneShard.getLogger.info("Creating the lucene shard")
    getLuceneShard
  }


}

object LuceneShard {

  @transient var log: Logger = null

  def getLogger: Logger = {
    if (log == null) {
      log = Logger.getLogger(classOf[LuceneShard])
    }
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