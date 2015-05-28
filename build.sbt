name := "ED1046_EDS"

version := "0.0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
	"org.apache.commons" % "commons-math3" % "3.2",
	"org.json4s" %% "json4s-native" % "3.2.11",
	"rengine" % "rengine" % "2015-01-20" from "http://rforge.net/Rserve/files/REngine.jar",
	"rserveengine" % "rserveengine" % "2015-01-20" from "http://rforge.net/Rserve/files/RserveEngine.jar",
	"org.slf4j" % "slf4j-api" % "1.7.7",
	"ch.qos.logback" % "logback-classic" % "1.1.1",
	"org.freemarker" % "freemarker" % "2.3.21"
	
)
