package au.csiro.variantspark.hail.variant.phased

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFConstants
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.Accumulable

import scala.collection.JavaConverters._
import scala.collection.mutable
import is.hail.io.vcf.HtsjdkRecordReader
import is.hail.io.vcf.GenericRecordReader

class PhasedGenericRecordReader(callFields: Set[String]) extends GenericRecordReader(callFields) {

  override def readRecord(reportAcc: Accumulable[mutable.Map[Int, Int], Int],
    vc: VariantContext,
    infoSignature: Option[TStruct],
    genotypeSignature: Type): (Variant, (Annotation, Iterable[Annotation])) = {

    val (v, va) = readVariantInfo(vc, infoSignature)
    val nAlleles = v.nAlleles

    val gs = vc.getGenotypes.iterator.asScala.map { g =>

      val alleles = g.getAlleles.asScala
      assert(alleles.length == 1 || alleles.length == 2,
        s"expected 1 or 2 alleles in genotype, but found ${ alleles.length }")
      val a0 = alleles(0)
      val a1 = if (alleles.length == 2)
        alleles(1)
      else
        a0

      assert(a0.isCalled || a0.isNoCall)
      assert(a1.isCalled || a1.isNoCall)
      assert(a0.isCalled == a1.isCalled)

      val gt = if (a0.isCalled) {
        val i = vc.getAlleleIndex(a0)
        val j = vc.getAlleleIndex(a1)
        GTPair.apply(i, j).p
        //Genotype.gtIndexWithSwap(i, j)
      } else null

      val a = Annotation(
        genotypeSignature.asInstanceOf[TStruct].fields.map { f =>
          val a =
            if (f.name == "GT")
              gt
            else {
              val x = g.getAnyAttribute(f.name)
              if (x == null || f.typ != TCall)
                x
              else {
                try {
                  GenericRecordReader.getCall(x.asInstanceOf[String], nAlleles)
                } catch {
                  case e: Exception =>
                    fatal(
                      s"""variant $v: Genotype field ${ f.name }:
                 |  unable to convert $x (of class ${ x.getClass.getCanonicalName }) to ${ f.typ }:
                 |  caught $e""".stripMargin)
                }
              }
            }

          try {
            HtsjdkRecordReader.cast(a, f.typ)
          } catch {
            case e: Exception =>
              fatal(
                s"""variant $v: Genotype field ${ f.name }:
                 |  unable to convert $a (of class ${ a.getClass.getCanonicalName }) to ${ f.typ }:
                 |  caught $e""".stripMargin)
          }
        }: _*)
      assert(genotypeSignature.typeCheck(a))
      a
    }.toArray

    (v, (va, gs))
  }
}