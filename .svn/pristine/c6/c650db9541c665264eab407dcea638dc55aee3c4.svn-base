package com.humedica.mercury.etl.test.support

import com.humedica.mercury.etl.core.engine.{Engine, EntitySource}
import com.typesafe.config.{ConfigList, ConfigObject, ConfigValueType}
import org.jgrapht.event.{ConnectedComponentTraversalEvent, EdgeTraversalEvent, TraversalListener, VertexTraversalEvent}
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.DepthFirstIterator

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class EntitySourceGraph(dummyConfig: Map[String, String]) extends TraversalListener[MercuryVertex, MercuryEdge] {

  val graph = new SimpleDirectedGraph[MercuryVertex, MercuryEdge](classOf[MercuryEdge])
  val edgeList = new ListBuffer[MercuryEdge]

  def apply(root: ConfigObject): EntitySourceGraph = {
    root.keys.flatMap(k0 => {
      val c0 = root.get(k0).asInstanceOf[ConfigObject]
      val vt0 = c0.valueType()
      vt0 match {
        case ConfigValueType.OBJECT =>
          c0.keys.flatMap(k1 => {
            val c1 = c0.get(k1)
            val vt1 = c1.valueType()
            vt1 match {
              case ConfigValueType.OBJECT =>
                c1.asInstanceOf[ConfigObject].keys.flatMap(k2 => {
                  val c2 = c1.asInstanceOf[ConfigObject].get(k2).asInstanceOf[ConfigList]
                  visitEntitySourceList(k0, k2, c2)
                })
              case ConfigValueType.LIST =>
                visitEntitySourceList(k0, k1, c1.asInstanceOf[ConfigList])
              case _ => Seq()
            }}
          )
        case _ => Seq()
      }
    })
    this
  }

  def visitEntitySourceList(emrName: String, entityName: String, entities: ConfigList): Seq[EntitySource] = {
    val fqen = s"${emrName}_$entityName"
    val emrVtx = new MercuryVertex(emrName, EmrImpl)
    val entityVtx = new MercuryVertex(fqen, Entity)
    graph.addVertex(emrVtx)
    graph.addVertex(entityVtx)
    graph.addEdge(emrVtx, entityVtx)
    entities.unwrapped().map(v => v.toString)
      .map(esName => Engine.getEntitySourceClass2(emrName, entityName, esName, dummyConfig))
      .filter(es => es != null)
      .flatMap(es => {
        val esList = visitEntitySource(es, dummyConfig)
        val transformVtx = new MercuryVertex(es.getClass.getCanonicalName, Xform)
        graph.addEdge(entityVtx, transformVtx)
        esList
      })
  }

  def visitEntitySource(es: EntitySource, dummyConfig: Map[String, String]): Seq[EntitySource] = {
    val myName = es.getClass.getCanonicalName
    val pseudoPkg = es.getClass.getCanonicalName.replace("." + es.getClass.getSimpleName, "")
    val me = Seq(es)
    val transformVtx = new MercuryVertex(myName, Xform)

    graph.addVertex(transformVtx)
    es.tables.filter(t => !t.contains(":"))
      .foreach(tn => {
        val tableVtx = new MercuryVertex(tn,
          if (tn.startsWith("cdr.") || tn.toUpperCase().startsWith("ZH"))
            MapT else InputT)
        graph.addVertex(tableVtx)
        graph.addEdge(transformVtx, tableVtx)
      })

    val childList = es.tables.filter(t => t.contains(":"))
      .flatMap(tAlias => {
        val elements = tAlias.split(":")
        val tableAlias = s"$pseudoPkg.${es.getClass.getSimpleName}.${elements(0)}"
        val tableAliasVtx = new MercuryVertex(tableAlias, TAlias)
        val es0 = Engine.getEntitySourceClass(tAlias, dummyConfig)
        val es0Vtx = new MercuryVertex(es0.getClass.getCanonicalName, Xform)
        val esList = visitEntitySource(es0, dummyConfig)
        graph.addVertex(tableAliasVtx)
        graph.addEdge(transformVtx, tableAliasVtx)
        graph.addEdge(tableAliasVtx, es0Vtx)
        return esList
      })

    val out = me ++ childList
    out
  }

  def listEdges(startVtx: MercuryVertex): Seq[MercuryEdge] = {
    edgeList.clear()
    val iter = new DepthFirstIterator[MercuryVertex, MercuryEdge](graph, startVtx)
    iter.addTraversalListener(this)
    iter.toList
    val result = Seq(edgeList: _*)
    edgeList.clear()
    result
  }

  override def edgeTraversed(e: EdgeTraversalEvent[MercuryEdge]): Unit = edgeList.add(e.getEdge)
  override def connectedComponentFinished(e: ConnectedComponentTraversalEvent): Unit = {}
  override def connectedComponentStarted(e: ConnectedComponentTraversalEvent): Unit = {}
  override def vertexTraversed(e: VertexTraversalEvent[MercuryVertex]): Unit = {}
  override def vertexFinished(e: VertexTraversalEvent[MercuryVertex]): Unit = {}
}
