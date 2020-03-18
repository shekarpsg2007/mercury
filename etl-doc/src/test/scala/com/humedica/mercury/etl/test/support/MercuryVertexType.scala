package com.humedica.mercury.etl.test.support

sealed abstract class MercuryVertexType()

case object EmrImpl extends MercuryVertexType
case object Entity extends MercuryVertexType
case object Xform extends MercuryVertexType
case object InputT extends MercuryVertexType
case object MapT extends MercuryVertexType
case object TAlias extends MercuryVertexType

class MercuryVertex(val label: String, val vertexType: MercuryVertexType) extends Comparable[MercuryVertex] {
  override def compareTo(o: MercuryVertex): Int = this.label.compareTo(o.label)
  override def hashCode(): Int = this.label.hashCode
  override def equals(obj: scala.Any): Boolean =
    obj match {
      case obj: MercuryVertex => obj.label.equals(this.label) && obj.vertexType.equals(this.vertexType)
      case _ => false
    }
  override def toString: String = s"${vertexType.getClass.getSimpleName} $label"
}
