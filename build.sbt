organization := "org.ddecap"

name := "rna-wxs-comparison"

homepage := Some(url(s"https://github.com/tmoerman/"+name.value))

scalaVersion := "2.10.4"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "bintray-tmoerman" at "http://dl.bintray.com/tmoerman/maven"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2" % "provided"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.+" % "provided"

libraryDependencies += "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.+" % "provided"

libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0" % "provided"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.6" % "provided"

libraryDependencies += "com.github.samtools" % "htsjdk" % "1.133" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.bdgenomics.utils" % "utils-misc_2.10" % "0.2.2" % "provided"

libraryDependencies += "org.tmoerman" % "adam-fx_2.10" % "0.5.5" % "provided"

fork in run := true

// bintray-sbt plugin properties

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintrayPackageLabels := Seq("scala", "adam", "genomics", "snpeff", "variants", "rna", "exome")
