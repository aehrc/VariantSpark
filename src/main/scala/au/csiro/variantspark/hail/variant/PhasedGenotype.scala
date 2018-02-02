package au.csiro.variantspark.hail.variant

import is.hail.variant.GTPair
import is.hail.variant.Genotype
import is.hail.variant.GenericGenotype
import is.hail.io.vcf.HtsjdkRecordReader
import htsjdk.variant.variantcontext.VariantContext
import is.hail.expr.TStruct
import is.hail.variant.Variant
import is.hail.expr.Type
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFConstants
import is.hail.annotations._
import is.hail.expr._
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.Accumulable
import is.hail.io.vcf.GenericRecordReader

import scala.collection.JavaConverters._
import scala.collection.mutable

class PhasedGenotype(val pair: GTPair, unboxedAD: Array[Int], unboxedDP: Int,
  unboxedGQ: Int, unboxedPX: Array[Int], fakeRef: Boolean, isLinearScale: Boolean) 
  extends GenericGenotype(Genotype.gtIndexWithSwap(pair.j, pair.k),
      unboxedAD,unboxedDP, unboxedGQ, unboxedPX, fakeRef, isLinearScale)

//case class PhasedGenericRecordReader() extends HtsjdkRecordReader[Genotype] {
//  def genericGenotypes = false
//
//  def readRecord(reportAcc: Accumulable[mutable.Map[Int, Int], Int],
//    vc: VariantContext,
//    infoSignature: Option[TStruct],
//    genotypeSignature: Type): (Variant, (Annotation, Iterable[Genotype])) = {
//
//    val (v, va) = readVariantInfo(vc, infoSignature)
//    val nAlleles = v.nAlleles
//
//    val gs = vc.getGenotypes.iterator.asScala.map { g =>
//
//      val alleles = g.getAlleles.asScala
//      assert(alleles.length == 1 || alleles.length == 2,
//        s"expected 1 or 2 alleles in genotype, but found ${ alleles.length }")
//      val a0 = alleles(0)
//      val a1 = if (alleles.length == 2)
//        alleles(1)
//      else
//        a0
//
//      assert(a0.isCalled || a0.isNoCall)
//      assert(a1.isCalled || a1.isNoCall)
//      assert(a0.isCalled == a1.isCalled)
//
//      //val gt = if (a0.isCalled) {
//      //  val i = vc.getAlleleIndex(a0)
//      //  val j = vc.getAlleleIndex(a1)
//      //  Genotype.gtIndexWithSwap(i, j)
//      //} else null
//
//      new PhasedGenotype(GTPair(vc.getAlleleIndex(a0), vc.getAlleleIndex(a1)), Array(), 0, 0,
//          Array(), false, false)
//          
//    }.toArray
//
//    (v, (va, gs))
//  }
//}
