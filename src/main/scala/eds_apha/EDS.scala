package eds_apha

import java.nio.file.{Files,Paths}
import java.time.YearMonth
import scala.annotation.elidable.ASSERTION
import scala.collection.immutable.TreeMap
import scala.io.Source
import scala.language.{postfixOps,implicitConversions}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.rosuda.REngine.Rserve.RConnection
import java.time._
import java.time.Year
import java.time.ZoneId
import java.time.temporal.ChronoUnit._
import java.time.temporal.ChronoField
import java.time.Duration.of
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit
import scala.collection.SortedMap
import org.json4s.JsonAST.JValue
import org.json4s.native.JsonMethods
import java.nio.charset.Charset
import java.nio.file.Path
import java.io.OutputStream
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.ConnectException
import java.nio.file.Files
import scala.annotation.tailrec
import scala.io.Source
import scala.sys.process.Process
import scala.sys.process.ProcessIO
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.language.implicitConversions
import org.rosuda.REngine.REXP
import org.rosuda.REngine.Rserve.RConnection
import freemarker.template.Configuration
import freemarker.template.TemplateExceptionHandler

case class Date(yearMonth: YearMonth, idx: Long)

case class Input(year: IndexedSeq[Int], month: IndexedSeq[Int], count: IndexedSeq[Int])
object Input{
  implicit val formats = DefaultFormats
  def apply(json: JValue): Input = Input(
      (json \ "year").extract[List[Int]].toIndexedSeq,
      (json \ "month").extract[List[Int]].toIndexedSeq,
      (json \ "count").extract[List[Int]].toIndexedSeq
    )
}

case class Result(date: Date, actual: Int, expected: Double, threshold: Double, trend: Int, exceed: Double, weights: IndexedSeq[Double]){
  lazy val isAlert = actual > threshold
}
object Result{
  implicit val formats = DefaultFormats
  def apply(date: Date, actual: Int, json: JValue): Result = Result(
      date,     
      actual,
      (json \ "expected").extract[Double],
      (json \ "threshold").extract[Double],
      (json \ "trend").extract[Int],    
      (json \ "exceed").extract[Double],
      (json \ "weights").extract[List[Double]].toIndexedSeq
    )
}

object EDS extends App{
	
  // Name of input and output files:
  val inputFile = "input.json"
	val outputFile = "results.json"
  
	val minimumBaselineObs = 108
	
	// Get and create working directories:
	val inputDir = Paths.get("dataIn")  
	val resultsDir = Paths.get("dataOut")
	Files.createDirectories(resultsDir)
  
  // Read and parse JSON file
  val jsonIn = parse(readJSON(inputDir.resolve(inputFile)))
  val input = Input(jsonIn)
  
  val countData = TreeMap{
    (0 until input.count.length).map{i =>
      YearMonth.of(input.year(i), input.month(i)) -> input.count(i)  
    }.toMap.toArray:_*
  }
   
  // Exclude 2001 data
  val exclude2001 = (1 to 12).map{m => YearMonth.of(2001, m)}.to[Set]
  val indexedData = indexAndExclude(countData, exclude2001)
  
  // Get length of indexed data:
  val nData = indexedData.map(i => i._2).toIndexedSeq.length
  
  // APHA Early Detection System
  RServeHelper.ensureRunning()
  val rCon = new RConnection
  val results = try{
    ((nData - minimumBaselineObs) to 0 by -1).map{i => 
      println(i)
      Farrington.run(extractWindow(indexedData.dropRight(i)), rCon)
    }
  } finally {
    rCon.close
    RServeHelper.shutdown
  }
  
  // Convert output to json and write to file
  val timeSeriesJSON = 
    ("source" -> inputFile) ~
    ("month" -> results.map(_.date.yearMonth.toString)) ~
    ("monthId" -> results.map(_.date.idx)) ~
    ("expected" -> results.map(_.expected)) ~
    ("threshold" -> results.map(_.threshold)) ~
    ("actual" -> results.map(_.actual))
  
  writeToFile(resultsDir.resolve(outputFile), pretty(render(timeSeriesJSON)))
  
//  // Plot html graph of actual counts and threshold calcs
  FreeMarkerHelper.writeFile(
    Map("jsonData" -> pretty(render(timeSeriesJSON))),
    "plot.ftl",
    resultsDir.resolve("output.html")
  )
  
  //==============
  // FUNCTIONS
  
  // Read json file as string
  def readJSON(path: Path): String = {
    val br = Files.newBufferedReader(path, Charset.defaultCharset())
    Stream.continually(br.readLine()).takeWhile(_ != null).mkString("\n")
  }
  
  def writeToFile(path: Path, script: String) = {
    val writer = Files.newBufferedWriter(path, Charset.defaultCharset())
    writer.write(script)
    writer.close()
  }
  
  def indexAndExclude(
      obsByDate: SortedMap[YearMonth, Int], 
      exclusions: Set[YearMonth] = Set.empty
  ): SortedMap[Date, Int] = {
    assert(!obsByDate.exists{case (ym, _) => exclusions.contains(ym)})    
    val removedExclusions = obsByDate.filterKeys{ym => !exclusions.contains(ym)}
    val firstDate = removedExclusions.firstKey    
    implicit val dateOrdering = Ordering.by{d: Date => d.idx}    
    removedExclusions.map{case (ym, count) => Date(ym, MONTHS.between(firstDate, ym)) -> count}
  }
  
  def extractWindow(timeSeries: SortedMap[Date, Int], nYears: Int = 12): SortedMap[Date, Int] = {
    val lastObsDate = timeSeries.lastKey
    val window = List(-1, 0, 1).map(v => (v + 12) % 12)
    val windowLowerBound = lastObsDate.yearMonth.minus(nYears, YEARS).minus(1, MONTHS)
    def keep(date: Date) = {
      val monthRemainder = MONTHS.between(date.yearMonth, lastObsDate.yearMonth) % 12
      val inWindow = window.exists(_ == monthRemainder)    
      val isAfterStartDate = windowLowerBound.compareTo(date.yearMonth) <= 0 
      val isBeforeEndDate = MONTHS.between(date.yearMonth, lastObsDate.yearMonth) > 2
      val isBaseline = inWindow && isAfterStartDate && isBeforeEndDate    
      isBaseline || date == lastObsDate
    }
    val t = timeSeries.filterKeys(keep)
    t
  }
  
  object FreeMarkerHelper{
  val cfg = new Configuration(Configuration.VERSION_2_3_21);
  
  val cl = getClass.getClassLoader
  val templateDir = Paths.get(cl.getResource("eds").toURI()).toFile()
  
  cfg.setDirectoryForTemplateLoading(templateDir);
  cfg.setDefaultEncoding("UTF-8");
  cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);  

  def writeFile(
      subs: Map[String, String], 
      tmplName: String, 
      path: Path
  ){
    val subsAsJava = collection.JavaConversions.mapAsJavaMap(subs)
    val out = Files.newBufferedWriter(path)
    cfg.getTemplate(tmplName).process(subsAsJava, out)
    out.close()
  }
}
  
}

object RServeHelper extends Logging{
  
  @tailrec
  def getConnection(countdown: Int): Try[RConnection] = {
    if(countdown == 0) Failure(new ConnectException("Could not connect to Rserve"))
    else try{
      val c = new RConnection()
      log.debug("Rserve connection confirmed")
      Success(c)
    } catch {
      case _: Exception => 
        val newCountdown = countdown - 1
        log.debug("Searching for Rserve {} tries left",newCountdown)
        Thread.sleep(100)
        getConnection(newCountdown)
    }
  }
  
  def ensureRunning(initialAttempts: Int = 1, postStartAttempts: Int = 10, daemonizeThreads: Boolean = true){
    getConnection(initialAttempts)
      .recoverWith{case _ => 
        startRserve(daemonizeThreads)
        getConnection(postStartAttempts)
      }
      .map(_.close())
      .recover{case _ => throw new Exception("Failed to find or start Rserve")}
      .get
  }
  
  private def startRserve(daemonizeThreads: Boolean){
    implicit def toLines(in: InputStream) = Source.fromInputStream(in).getLines
    
    log.info("Starting new Rserve process (daemon = {})", daemonizeThreads)
    val io = new ProcessIO(
          in => in.close,
          out => {
            out.foreach(log.info)
            out.close 
          },
          err => {
            err.foreach(log.error)
            err.close 
          },
          daemonizeThreads
        )
    Process("R CMD Rserve --no-save --slave").run(io)
  }
  
  def runScript(script: String, saveToFile: Option[Path] = None): Try[REXP] = {
    saveToFile.map{filePath => 
      val writer = Files.newBufferedWriter(filePath)
      writer.write(script)
      writer.newLine
      writer.close
    }    
    getConnection(1).map{connection =>
      try{
        connection.parseAndEval(script)
      }finally{
        connection.close
      }
    }
  }
  
  def shutdown() = new RConnection().shutdown()
}

trait Logging{
  lazy val log = LoggerFactory.getLogger(getClass)
}

object Farrington {
  
  val cl = getClass.getClassLoader
  val rScript = Source.fromURI(cl.getResource("eds/script_apha.r").toURI()).mkString
    
  def run(dataIn: SortedMap[Date, Int], rCon: RConnection): Result = {
    
    val json = buildJSON(dataIn)
    val jsonAsString = pretty(render(json))
    
    val rExpression = {      
      import rCon._
      parseAndEval("""library(rjson)""")
      assign("jsonIn", compact(render(json)))
      parseAndEval("basedata = as.data.frame(fromJSON(jsonIn)$Baseline)")
      parseAndEval("currentCount = as.data.frame(fromJSON(jsonIn)$Current$Incidents)")
      parseAndEval("currentmth = as.data.frame(fromJSON(jsonIn)$Current$Month)")
      parseAndEval("startdte = as.data.frame(fromJSON(jsonIn)$StartDate)")
      parseAndEval(rScript)
      parseAndEval("output")
    }      
    val rOut = parse(rExpression.asString())
    val (date, value) = dataIn.last    
    
    Result(date, value, rOut)    
  }
  
  def buildJSON(timeSeries: SortedMap[Date, Int]): JObject = {
    val now = timeSeries.last
    val history = timeSeries
//      if (Mode == APHA) timeSeries.dropRight(1)
//      else timeSeries
    val firstDate = timeSeries.head._1
    
    val t = now._1.idx
    val r = now._2
    
    ("Current" -> 
      ("Month" -> now._1.idx) ~
      ("Incidents" -> now._2)
    ) ~
    ("Baseline" ->
      ("basemth" -> history.keySet.map(_.idx) ) ~
      ("basecont"-> history.values)
    ) ~
    ("StartDate" ->
      ("year" -> firstDate.yearMonth.getYear) ~
      ("month" -> firstDate.yearMonth.getMonthValue)
    )
  }
  
}