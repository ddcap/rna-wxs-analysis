package org.ddecap.snv

import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ShuffleRegionJoin
import org.bdgenomics.formats.avro._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration 
import org.tmoerman.adam.fx.snpeff.SnpEffContext
import org.bdgenomics.adam.models.SequenceDictionary
import htsjdk.samtools.SAMSequenceRecord
import htsjdk.samtools.SAMSequenceDictionary
import org.apache.spark.SparkContext
import scalax.io.Resource
import scalax.io._


object analysis {
val matchingDpHist = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.title = "% variants"
      var x =chart.addCategoryAxis("x", ["_1", "_2"]);
      x.title = "RD in matching sample"
      var s = chart.addSeries("_2", dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val RocGraphA = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_A"
      var x =chart.addMeasureAxis("x", "_2");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      chart.addSeries("_1", dimple.plot.line);
      chart.draw();
}
"""
val RocGraphB = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_5");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_B"
      var x =chart.addMeasureAxis("x", "_4");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      chart.addSeries("_1", dimple.plot.line);
      chart.draw();
}
"""
val RocGraph2A = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_4");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_A"
      var x =chart.addMeasureAxis("x", "_3");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      var s = chart.addSeries(["_2", "_1"], dimple.plot.line);
      s.addOrderRule("_1")
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val RocGraph2B = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_6");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_B"
      var x =chart.addMeasureAxis("x", "_5");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      var s = chart.addSeries(["_2", "_1"], dimple.plot.line);
      s.addOrderRule("_1")
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val RocGraphOutA = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_4");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_A"
      var x =chart.addMeasureAxis("x", "_3");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      var s = chart.addSeries("_3", dimple.plot.line);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val RocGraphOutB = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_6");
      y.overrideMin = 0
      y.overrideMax = 1
      y.title = "1 - FPR_A"
      var x =chart.addMeasureAxis("x", "_5");
      x.overrideMin = 0
      x.overrideMax = 1
      x.title = "1 - TPR"
      var s = chart.addSeries("_5", dimple.plot.line);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val QHist = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.title = "normalized % variants"
      var x =chart.addLogAxis("x", "_2");
      x.title = "Quality"
      var s = chart.addSeries(["_2", "_1"], dimple.plot.line);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val QBDHist = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.title = "normalized % variants"
      var x =chart.addMeasureAxis("x", "_2");
      x.title = "QualityByDepth"
      var s = chart.addSeries(["_2", "_1"], dimple.plot.line);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val annoFreqGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addLogAxis("y", "_3");
      y.title = "# variants"
      var x =chart.addCategoryAxis("x", ["_2", "_1"]);
      x.title = "annotation type"
      chart.addSeries("_1", dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val annoFreqBarGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.title = "# variants"
      var x =chart.addCategoryAxis("x", "_2");
      x.title = "annotation type"
      chart.addSeries("_1", dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val alleleFreqGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_3");
      y.title = "rna allele freq"
      var x =chart.addMeasureAxis("x", "_2");
      x.title = "wxs allele freq"
      chart.addSeries(["_2","_1"], dimple.plot.line);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val alleleFreqScGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 20, 480, 330)
      var y =chart.addMeasureAxis("y", "_2");
      y.title = "rna allele freq"
      var x =chart.addMeasureAxis("x", "_3");
      var z =chart.addMeasureAxis("z", "_4");
      x.title = "wxs allele freq"
      chart.addSeries(["_2","_3"], dimple.plot.bubble);
      chart.draw();
}
"""
val gqGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(60, 30, 480, 330)
      var x = chart.addMeasureAxis("x", "threshold");
      x.ticks = 10
      x.title = "Genome Quality threshold";
      var y = chart.addMeasureAxis("y", "count");
      y.title = "# variants"
      var s = chart.addSeries(["threshold", "group"], dimple.plot.line);
      s.lineWeight = 1;
      s.barGap = 0.05;
      chart.addLegend(550, 20, 100, 300, "left");
      chart.draw();
}
"""
val rpkmGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(60, 30, 480, 330)
      var x = chart.addLogAxis("x", "threshold");
      x.title = "RPKM threshold";
      x.ticks = 10
      var y = chart.addMeasureAxis("y", "count");
      y.title = "# variants"
      var s = chart.addSeries(["threshold", "group"], dimple.plot.line);
      s.lineWeight = 1;
      s.barGap = 0.05;
      chart.addLegend(550, 20, 100, 300, "left");
      chart.draw();
}
"""
val dpGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(60, 30, 480, 330)
      var x = chart.addLogAxis("x", "threshold");
      x.title = "Depth threshold";
      x.ticks = 10
      x.overrideMin = 1
      var y = chart.addMeasureAxis("y", "count");
      y.title = "# variants"
      var s = chart.addSeries(["threshold", "group"], dimple.plot.line);
      s.lineWeight = 1;
      s.barGap = 0.05;
      chart.addLegend(550, 20, 100, 300, "left");
      chart.draw();
}
"""
val qGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(60, 30, 480, 330)
      var x = chart.addLogAxis("x", "threshold");
      x.title = "Quality threshold";
      x.ticks = 10;
      x.overrideMin = 1;
      var y = chart.addMeasureAxis("y", "count");
      y.title = "# variants"
      var s = chart.addSeries(["threshold", "group"], dimple.plot.line);
      s.lineWeight = 1;
      s.barGap = 0.05;
      chart.addLegend(550, 20, 100, 300, "left");
      chart.draw();
}
"""
val splitBsGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 30, 480, 330)
      chart.addPctAxis("y", "count");
      chart.addCategoryAxis("x", ["reference", "group"]);
      chart.addSeries(["reference", "alternate"], dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val mergedBsGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 30, 480, 330)
      chart.addMeasureAxis("y", "count");
      chart.addCategoryAxis("x", ("group". "threshold"));
      chart.addSeries("baseChange", dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""
val bChangesGraph = """
function(data, headers, chart /*dimple: http://dimplejs.org/*/) {
      chart.setBounds(80, 30, 480, 330)
      chart.addMeasureAxis("y", "_3");
      chart.addCategoryAxis("x", ["_1", "_2"]);
      chart.addSeries("_2", dimple.plot.bar);
      chart.addLegend(200, 10, 380, 20, "right");
      chart.draw();
}
"""

val lgOf2 = math.log(2)

	case class GenotypeWithMetadata(
		genotype: org.bdgenomics.formats.avro.Genotype, 
		annotations: org.tmoerman.adam.fx.avro.SnpEffAnnotations, 
		rpkm:Double,
		otherCoverage: Long)

	def isSingleNucleotideVariant(g: org.bdgenomics.formats.avro.Genotype): Boolean = {
		g.getVariant().getReferenceAllele().length == 1 && g.getVariant().getAlternateAllele().length == 1
	}

	def filterGenotypes(genotypes: RDD[Iterable[GenotypeWithMetadata]], 
					mDepthFilter: Double = 0,
		            depth: Double = 0, 
		            gq: Double = 0, 
		            qual: Double= 0, 
		            qd: Double = 0, 
		            rpkm: Double = 0): RDD[Iterable[GenotypeWithMetadata]] = {
	  genotypes.filter(x => ! x.filter(v => (v.otherCoverage >= mDepthFilter &&
		                                     v.genotype.getReadDepth >= math.max(depth, mDepthFilter) &&
		                                     v.genotype.getGenotypeQuality >= gq &&
		                                     v.genotype.getVariantCallingAnnotations.getVariantCallErrorProbability >= qual &&
		                                     v.rpkm >= rpkm &&
		                                     v.genotype.getVariantCallingAnnotations.getVariantQualityByDepth >= qd)).isEmpty)
	}

	def qualityByDeptHist(genotypes: RDD[Iterable[GenotypeWithMetadata]], name: String) : RDD[(String, Double, Double)] = {
		val gCount = genotypes.count.toFloat
		genotypes.map(x => (math.round(x.maxBy(_.genotype.getVariantCallingAnnotations.getVariantQualityByDepth)
   				 .genotype.getVariantCallingAnnotations.getVariantQualityByDepth*2)/2.0, 1)).reduceByKey(_+_)
                 .map(x => (name, x._1, x._2/gCount))
	}	
	def binQuality(q: Double): Double ={
	  math.round(math.pow(2, math.ceil(math.log(q)/(lgOf2/4))/4))
	}
	def qualityHist(genotypes: RDD[Iterable[GenotypeWithMetadata]], name: String) : RDD[(String, Double, Double)] = {
		val gCount = genotypes.count.toFloat
		genotypes.map(x => (binQuality(x.maxBy(_.genotype.getVariantCallingAnnotations.getVariantCallErrorProbability)
                                                .genotype.getVariantCallingAnnotations.getVariantCallErrorProbability.toDouble), 1)) 					 .reduceByKey(_+_)
                 .map(x => (name, x._1, x._2/gCount))
	}
	def annotationCount(genotypes: RDD[Iterable[GenotypeWithMetadata]], name: String) : RDD[(String, String, Int)] = {
		genotypes.map(x => x.head.annotations.getFunctionalAnnotations.head.getAnnotations.head)
                 .map(x => (x, 1)).reduceByKey(_ + _)
                 .map{case (x,r) => (name,x,r)}
	}
	def isPyrimidine(nucleotyide: String) : Boolean = {
		return nucleotyide == "C" || nucleotyide == "T"
	}
	def isTransition(from: String, to: String) : Boolean = {
		return isPyrimidine(from) == isPyrimidine(to) 
	}
	def getTiTvRatio(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => isTransition(x.head.genotype.getVariant.getReferenceAllele, 
										   x.head.genotype.getVariant.getAlternateAllele))
				 .count.toDouble / genotypes.filter(x => ! isTransition(x.head.genotype.getVariant.getReferenceAllele, 
															   x.head.genotype.getVariant.getAlternateAllele)).count.toDouble
	}
	def getMultiAllelicSNVsFraction(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => x.size > 1).count.toDouble / genotypes.count.toDouble
	}
	def getHomozygousFraction1(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => {
                            	val all = x.head.genotype.getAlleles
                            	all(0) == all(1)}).count.toDouble / genotypes.count.toDouble
	}
	def getHomozygousFraction2(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => x.head.genotype.getAlternateReadDepth.toFloat == x.head.genotype.getReadDepth).count.toDouble / genotypes.count.toDouble
	}
	def getRareFraction(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => x.head.annotations.getDbSnpAnnotations != null).count.toDouble / genotypes.count.toDouble
	}
	def getClinvarFraction(genotypes: RDD[Iterable[GenotypeWithMetadata]]) : Double = {
		genotypes.filter(x => x.head.annotations.getClinvarAnnotations != null).count.toDouble / genotypes.count.toDouble
	}
	def alleleFrequency(genotypes: RDD[Iterable[GenotypeWithMetadata]], name: String) : RDD[(String, Double, Int)] = {
		genotypes.map(x => ("%.1f".format(x.head.genotype.getAlternateReadDepth.toFloat / x.head.genotype.getReadDepth) ,1))
                 .reduceByKey(_ + _)
                 .map{ case (freq, count) => (name,freq.toDouble,count)}
	}
	def scatterAlleleFrequency(genotypes: RDD[(Iterable[GenotypeWithMetadata], Iterable[GenotypeWithMetadata])], name: String) : RDD[(String, Double, Double, Int)] = {
		genotypes.map(x => 
                        (x._1.head.genotype.getAlternateReadDepth.toFloat / 
                         (x._1.head.genotype.getAlternateReadDepth + x._1.head.genotype.getReferenceReadDepth),
                         x._2.head.genotype.getAlternateReadDepth.toFloat / 
                         (x._2.head.genotype.getAlternateReadDepth + x._2.head.genotype.getReferenceReadDepth)))
                       .map{ case (x, y) => ((name,"%.1f".format(x),"%.1f".format(y)),1)}
                       .filter(x => x._1._2 != "NaN" && x._1._3 != "NaN")
                       .reduceByKey(_ + _)
                       .map{case (x, y) => (x._1, x._2.toDouble, x._3.toDouble, y.toInt)}
	}
	def baseChanges(genotypes: RDD[Iterable[GenotypeWithMetadata]], name: String) : RDD[(String, String, Long)] = {
		genotypes.map(g => ((g.head.genotype.getVariant.getReferenceAllele, g.head.genotype.getVariant.getAlternateAllele), 1))
                 .reduceByKey(_+_)
				 .map(x => (name, x._1._1+">"+x._1._2, x._2))
	}
	def UniquifyNoAllele(genotype: Genotype): String = {
	  val v = genotype.getVariant
	  genotype.getSampleId + "@" +v.getContig.getContigName+"+"+v.getStart
	}

	def Uniquify(genotype: Genotype): String = {
	  val v = genotype.getVariant
	  genotype.getSampleId + "@" +v.getContig.getContigName+"+"+v.getStart+":"+
	  v.getReferenceAllele + ">" +v.getAlternateAllele
	}

	def readSnvWithMeta(sc: SparkContext, input: String, rpkmFile: String, mCovFile: String): RDD[(String, GenotypeWithMetadata)] = {
	  val rpkm = sc.textFile(rpkmFile).map(x => x.split("\t")).map(x => (x(0), x(1).toDouble))
	  val mCov = sc.textFile(mCovFile).map(x => x.split("\t")).map(x => (x(0), x(1).toLong))
	  val ec = new SnpEffContext(sc)
	  val genotypes = ec.loadAnnotatedGenotypes(input).map(x => (UniquifyNoAllele(x.getGenotype), x))
	  val genotypesWithRpkm = genotypes.leftOuterJoin(rpkm)
		                               .map{ case (id, (g, rpkm)) => (id, (g, rpkm.getOrElse(0.0))) }
	  genotypesWithRpkm.leftOuterJoin(mCov)
		               .map{ case (id, ((g, r), mcov)) => 
		                    (UniquifyNoAllele(g.getGenotype), 
		                     GenotypeWithMetadata(g.getGenotype, g.getAnnotations, r, mcov.getOrElse(0L))) }
	}

	def getOverlapWithBed(seqDict: SequenceDictionary,
                      genotypes: RDD[Iterable[GenotypeWithMetadata]],
                      bedRDD: RDD[ReferenceRegion]): RDD[Iterable[GenotypeWithMetadata]] = {
	  val posRDD = genotypes.map(g => (ReferenceRegion(g.head.genotype.getVariant.getContig.getContigName, 
		                                           g.head.genotype.getVariant.getStart, 
		                                           g.head.genotype.getVariant.getEnd), g))
	  val maxPartitions = math.max(bedRDD.partitions.length.toLong, posRDD.partitions.length.toLong)

	  val joinedRDD = ShuffleRegionJoin(seqDict, seqDict.records.map(_.length).sum / maxPartitions)
		                .partitionAndJoin(posRDD, bedRDD.map(v => (v,1)))
	  joinedRDD.reduceByKey(_+_).map(x => x._1)
	}
}
