package com.github_zhu.gmall.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Index, Search, SearchResult}
import org.elasticsearch.index.engine.Engine.Searcher
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.mutable.ListBuffer

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/11 15:09
 * @ModifiedBy:
 *
 */
object MyEsUtil {

  private var factory: JestClientFactory = null

  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject
  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000)
      .build())
  }

  def main(args: Array[String]): Unit = {

    val jest: JestClient = getClient

    //    val actorList: ListBuffer[Map[(String,Long),(String,String)]] = new ListBuffer[Map[(String,Long),(String,String)]]
    //    actorList.append(Map(("id",1L),"name","""Tim"""))
    //    val mo: Movie1 = Movie(1, "肖申克的救赎", 9.2,)
    /* val actorList: ListBuffer[Map[Long, String]] = new ListBuffer[Map[Long, String]]
     actorList.append(Map(1L -> "Tim"))
     actorList.append(Map(2L -> "Jack"))
     val mo: Movie = Movie(1, "肖申克的救赎", 9.2, actorList)
     val index: Index = new Index.Builder(mo).`type`("movie").index("movie_chn").id("7") build()*/
    val query = "{\n  \"query\": {\n    \"match\": {\n      \"actorList.name\": \"海清\"\n    }\n  }\n}"
    val search: Search = new Search.Builder(query).addIndex("movie_chn").addType("movie").build()
    val builder: SearchSourceBuilder = new SearchSourceBuilder

    builder.query(new MatchQueryBuilder("name","红海战役"))
    builder.sort("doubanScore",SortOrder.ASC)
    val query2: String = builder.toString

    val search2: Search = new Search.Builder(query2).addIndex("movie_chn").addType("movie").build()
    val searchRes: SearchResult = jest.execute(search2)

    import scala.collection.JavaConversions._
      val movieList: util.List[SearchResult#Hit[Movie, Void]] = searchRes.getHits(classOf[Movie])
      val movies: ListBuffer[Movie] = ListBuffer[Movie]()

      for (hit <- movieList) {
        val movie: Movie = hit.source
        movies += movie
      }
      println(movies.mkString(","))

    jest.close()
  }

  //actorList: List[Long,String]
  //1122
  case class Movie(id: Long, name: String, doubanScore: Double) {}

  //  case class Movie(id: Long, name: String, doubanScore: Double, actorList: ListBuffer[Map[Long, String]]) {}

    case class Movie1(id: Long, name: String, doubanScore: Double, actorList: ListBuffer[Map[(String, Long), (String, String)]]) {}

}
