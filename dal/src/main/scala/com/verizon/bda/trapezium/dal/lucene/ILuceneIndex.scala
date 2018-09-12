package com.verizon.bda.trapezium.dal.lucene

import java.util

import org.apache.lucene.search.ScoreDoc
import org.apache.spark.sql.Row

/**
  * Created by sankma8 on 6/20/18.
  */
trait ILuceneIndex {

  def searchDocs(queryStr: String,
                 sample: Double = 1.0): util.BitSet

  def searchDocsWithRelevance(queryStr: String, sample: Double = 1.0): Array[ScoreDoc]

  /* TODO: Generate a document iterator over BitSet
    var i = docs.nextSetBit(0)
    while (i >= 0) {
      val d = docs.nextSetBit(i)
      Transform(d)
      i = docs.nextSetBit(i + 1)
    }
  */
  def search(queryStr: String, columns: Seq[String], sample: Double = 1.0): Iterator[Row]

  // If timestamp < minTime skip, timestamp >= maxTime skip
  // TODO: For numeric fields, they can be passed through such filters as well, look into
  // integrating time filter as dataframe filter
  def filter(docs: util.BitSet,
             column: String,
             min: Long,
             max: Long): util.BitSet

  // TODO: Multiple measures can be aggregated
  // TODO: Generalize it to dataframe aggregate
  def aggregate(queryStr: String,
                measure: String,
                agg: OLAPAggregator): OLAPAggregator

  //TODO: Use term dictionary to extract dimension and avoid dv seeks
  def facet(queryStr: String,
            dimension: String,
            dimOffset: Int,
            agg: OLAPAggregator): OLAPAggregator

  // TODO: Generalize it to Dataframe groupBy
  def group(queryStr: String,
            dimension: String,
            dimOffset: Int,
            measure: String,
            minTime: Long = 0L,
            maxTime: Long = Long.MaxValue,
            agg: OLAPAggregator): OLAPAggregator

  // Given a timestamp, find timestamp - minTime / rollup to find the index
  // statistics of the timestamp column should give minTime and maxTime
  // user specified min and max time overwrite the column stats
  def timeseries(queryStr: String,
                 minTime: Long,
                 maxTime: Long,
                 rollup: Long,
                 measure: String,
                 agg: OLAPAggregator): OLAPAggregator
}
