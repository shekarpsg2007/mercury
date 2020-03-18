package com.humedica.mercury.etl.test.support

import com.humedica.mercury.etl.core.engine.{Constants, Engine, EntitySource}
import com.humedica.mercury.etl.core.util.Notes
import com.typesafe.config.{ConfigList, ConfigObject, ConfigValueType}
import org.joox.JOOX.$
import org.joox.Match

import scala.io.Source
import scala.collection.JavaConversions._

class EntitySourceSummary(dummyConfig: Map[String, String], graph: EntitySourceGraph) {

  def apply(root: ConfigObject): String = {
    val out = $(Source.fromInputStream(getClass.getResourceAsStream("/doc-template.html")).mkString,
      html(root.keys.toList.sorted.map(k0 => {
        val c0 = root.get(k0).asInstanceOf[ConfigObject]
        val vt0 = c0.valueType()
        vt0 match {
          case ConfigValueType.OBJECT => div("emrImpl", Seq(
            h2(s"EMR implementation: <code>$k0</code>")) ++
            c0.keys.toList.sorted.map(k1 => {
              val c1 = c0.get(k1)
              val vt1 = c1.valueType()
              vt1 match {
                case ConfigValueType.OBJECT =>
                  div("emrConfig", Seq(
                    h3(s"Configuration: <code>$k1</code>")) ++
                    c1.asInstanceOf[ConfigObject].keys.toList.sorted.map(k2 => {
                      val c2 = c1.asInstanceOf[ConfigObject].get(k2).asInstanceOf[ConfigList]
                      visitList(k0, k1, k2, c2)
                    }))
                case ConfigValueType.LIST => visitList(k0, "", k1, c1.asInstanceOf[ConfigList])
              }
            })
          )
        }
      }))
    )
    out.toString
  }

  def visitList(emrName: String, configName: String, entityName: String, entities: ConfigList): Match = {
    val startVtx = s"${emrName}_$entityName"
    val graphId = s"${emrName}_${configName}_$entityName"
    val edges = graph.listEdges(new MercuryVertex(startVtx, Entity))
    val nodes = edges.flatMap(e0 => Seq(e0.getFrom(), e0.getTo())).toSet
    val visNode = div(Seq(
      div(graphId, "visNode", "", Seq()),
      script(
        s"""
          |var ${graphId}Data = {
          |  nodes: [
          |    ${nodes.map(n0 => s"{id: '${n0.label}', label: '${n0.label.replace(dummyConfig(Constants.PACKAGE_ROOT) + ".", "")}', group: '${n0.vertexType}'}").mkString(",\n")}
          |  ],
          |  edges: [
          |    ${edges.map(e0 => s"{from: '${e0.getFrom().label}', to: '${e0.getTo().label}', color:{ inherit:'from' }}").mkString(",\n")}
          |  ]
          |};
          |var ${graphId}Container = document.getElementById('$graphId');
          |new vis.Network(${graphId}Container, ${graphId}Data, optionsIO);
        """.stripMargin)
    ))
    val result = div("entity",
      Seq(h3(s"Entity <code>$entityName</code>")) ++
      (if (nodes.nonEmpty) Seq(visNode) else Seq()) ++
      entities.unwrapped().toList.map(v => v.toString).sorted
        .map { esName =>
          renderEntitySource(esName, Engine.getEntitySourceClass2(emrName, entityName, esName, dummyConfig))
        })
    result
  }

  def renderEntitySource(esName: String, es: EntitySource): Match =
    if (es == null)
      p(s"$esName -> No implementation available")
    else {
      val notes = es.getClass.getAnnotation(classOf[Notes])
      div(Seq(
        if (notes != null && notes.value() != "")
          div("rt1col", Seq(note(notes.value())))
        else div(Seq()),
        div("rt rt4col", Seq(
          renderInputTables(esName, es),
          renderInputTableColumns(es),
          renderJoin(es), renderMap(es)
        ))
      ))
    }

  def renderInputTables(esName: String, es: EntitySource): Match = {
    val notes = es.getClass.getAnnotation(classOf[Notes])
    div("rtCell", Seq(
      h4(esName),
      h5("Input Tables"),
      ul(es.tables.sorted.map(tName => li(tName))),
      if (notes != null && notes.tables() != "") note(notes.tables()) else div(Seq())
    ))
  }

  def renderInputTableColumns(es: EntitySource): Match = {
    val notes = es.getClass.getAnnotation(classOf[Notes])
    div("rtCell", Seq(
      h4("Input table columns"),
      ul(es.columnSelect.keys.toList.sorted.map(tName =>
        li(Seq(h4(tName), ul(es.columnSelect(tName).map(cCol => li(cCol)))))
      )),
      if (notes != null && notes.columnSelect() != "") note(notes.columnSelect()) else div(Seq())
    ))
  }

  def renderJoin(es: EntitySource): Match = {
    val notes = es.getClass.getAnnotation(classOf[Notes])
    div("rtCell", Seq(
      h4("Inclusion, Exclusion, and De-duping"),
      ul(es.beforeJoin.keys.toList.sorted.map(tName => li(tName))),
      if (notes != null && notes.beforeJoin() != "") note(notes.beforeJoin()) else div(Seq()),
      if (es.beforeJoinExceptions.nonEmpty) {
        div(Seq(
          h4("Before Join Exceptions"),
          ul(es.beforeJoinExceptions.keys.toList.sorted.map(tName => li(tName))),
          if (notes != null && notes.beforeJoinExceptions() != "") note(notes.beforeJoinExceptions()) else div(Seq())
        ))
      } else { div(Seq()) },
      if (notes != null && notes.join() != "") note(notes.join()) else div(Seq()),
      if (es.joinExceptions.nonEmpty) {
        div(Seq(
          h4("Join Exceptions"),
          ul(es.joinExceptions.keys.toList.sorted.map(tName => li(tName))),
          if (notes != null && notes.joinExceptions() != "") note(notes.joinExceptions()) else div(Seq())
        ))
      } else { div(Seq()) },
      if (notes != null && notes.afterJoin() != "") note(notes.afterJoin()) else div(Seq()),
      if (es.afterJoinExceptions.nonEmpty) {
        div(Seq(
          h4("After Join Exceptions"),
          ul(es.afterJoinExceptions.keys.toList.sorted.map(tName => li(tName))),
          if (notes != null && notes.afterJoin() != "") note(notes.afterJoin()) else div(Seq()),
          if (notes != null && notes.afterJoinExceptions() != "") note(notes.afterJoinExceptions()) else div(Seq())
        ))
      } else { div(Seq()) }
    ))
  }

  def renderMap(es: EntitySource): Match = {
    val notes = es.getClass.getAnnotation(classOf[Notes])
    div("rtCell", Seq(
      h4("Target Mapping Columns"),
      ul(es.map.keys.toList.map(mCol => li(mCol))),
      if (notes != null && notes.map() != "") note(notes.map()) else div(Seq()),
      if (es.mapExceptions.nonEmpty) {
        div(Seq(
          h4("Mapping Exceptions"),
          ul(es.mapExceptions.keys.toList.map(ex => ex.toString()).sorted.map(tName => li(tName))),
          if (notes != null && notes.mapExceptions() != "") note(notes.mapExceptions()) else div(Seq())
        ))
      } else { div(Seq()) },
      if (notes != null && notes.afterMap() != "") note(notes.afterMap()) else div(Seq()),
      if (es.afterMapExceptions.nonEmpty) {
        div(Seq(
          h4("After Mapping Exceptions"),
          ul(es.afterMapExceptions.keys.toList.sorted.map(tName => $(li(tName)))),
          if (notes != null && notes.afterMapExceptions() != "") note(notes.afterMapExceptions()) else div(Seq())
        ))
      } else { div(Seq()) },
      if (es.columns.nonEmpty) {
        div("rtCell", Seq(
          h4("Direct Output Columns"),
          ul(es.columns.sorted.map(tName => li(tName)))
        ))
      } else { div(Seq()) }
    ))
  }

  def html(content: Seq[Match]): Match = $("<html />", content: _*)
  def h2(title: String): Match = $("<h2 />", title)
  def h3(title: String): Match = $("<h3 />", title)
  def h4(title: String): Match = $("<h4 />", title)
  def h5(title: String): Match = $("<h5 />", title)

  def attr(name: String, value: String): String = s"${if (value.ne("")) s"$name='$value'" else ""}"

  def div(id: String, classes: String, style: String, content: Seq[Match]): Match =
    $(s"<div ${attr("id", id)} ${attr("class", classes)} ${attr("style", style)} />".toString, content: _*)
  def div(classes: String, content: Seq[Match]): Match = div("", classes, "", content)
  def div(content: Seq[Match]): Match = div("", "", "", content)

  def ul(content: Seq[Match]): Match = $("<ul class=\"colList\" />", content: _*)
  def li(content: String): Match = $("<li />", content)
  def li(content: Seq[Match]): Match = $("<li />", content: _*)
  def p(content: String): Match = $("<p />", content)
  def script(content: String): Match = $("<script />", content)

  def note(content: String): Match = div("notes", Seq(
    h5("Notes"),
    $("<code />", content)
  ))
}
