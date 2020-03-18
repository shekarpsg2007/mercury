package com.humedica.mercury.etl.test.support

import org.jgrapht.graph.DefaultEdge

class MercuryEdge extends DefaultEdge {
  def getFrom(): MercuryVertex = getSource.asInstanceOf[MercuryVertex]
  def getTo(): MercuryVertex = getTarget.asInstanceOf[MercuryVertex]
}
