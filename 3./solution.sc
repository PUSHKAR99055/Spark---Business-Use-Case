import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.HashMap

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("Phrase Mining Example")
  .master("local[*]") // Change to your cluster settings
  .getOrCreate()

// Load data into DataFrame
val sentencesDf = spark.read
  .format("text")
  .load("/tmp/gensim-input")
  .as[String]

// Define the Phrase mining function (mock implementation for example)
object Phrases {
  def learnVocab(sentence: String): HashMap[String, Int] = {
    // Mock implementation: counts occurrences of each word
    val words = sentence.split(" ")
    val vocab = new HashMap[String, Int]()
    words.foreach(word => vocab.put(word, vocab.getOrElse(word, 0) + 1))
    vocab
  }
}

// Cache the DataFrame to speed up iterative processing
sentencesDf.cache()

// Process each partition to mine phrases
val partitionCorpusDf = sentencesDf.mapPartitions(sentencesInPartitionIter => {
  val partitionCorpus = new HashMap[String, Int]()

  while (sentencesInPartitionIter.hasNext) {
    val sentence = sentencesInPartitionIter.next()
    val sentenceCorpus = Phrases.learnVocab(sentence)
    sentenceCorpus.foreach { case (phrase, count) =>
      partitionCorpus.put(phrase, partitionCorpus.getOrElse(phrase, 0) + count)
    }
  }
  
  partitionCorpus.iterator
})

// Aggregate phrase counts from all partitions
val globalCorpus = new HashMap[String, Int]()
val aggregatedCorpus = partitionCorpusDf.collect()

aggregatedCorpus.foreach { case (phrase, count) =>
  val globalCount = globalCorpus.getOrElse(phrase, 0)
  globalCorpus.put(phrase, globalCount + count)
}

// Convert the global corpus to a DataFrame for snapshot
import spark.implicits._
val globalCorpusDf = globalCorpus.toSeq.toDF("Phrase", "Count")

// Save snapshot of the global phrase counts
globalCorpusDf.write
  .format("parquet")
  .mode("overwrite")
  .save("/tmp/global-phrase-snapshot")

// Stop SparkSession
spark.stop()
