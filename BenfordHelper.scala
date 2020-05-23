package com.random.spark.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

/*
Applies Benford's Law in Spark UDAFs using only the first significant digit (FSD)

Definitions
Numeric column: DataFrame column with numeric data type
Benford counts: Distribution of raw counts of FSDs as an array
Normalised Benford counts: Benford counts normalised to sum to 1
Error score: Measures distance between actual Benford counts and expected distribution according to Benford's Law. 0 means perfect fit

Options
considerZero: Defaults to false as Benford's Law doesn't consider zero a first significant digit. Either way we still count it for values that are exactly zero, but if false, zero values aren't included in sample size, normalised sum or error score. But if your data can have zero values that you don't want to ignore, might be better to set this to true
*/

object BenfordHelper extends BenfordStuff {
  // for convenience, but you can also ignore this section and define your own

  // Numeric column => Benford counts
  val benfordCounts = new BenfordCounts
  // Numeric column => Normalised Benford counts
  val benfordProbs = new BenfordProbs(considerZero=false)
  // Numeric column => Error score using Cho–Gaines's d
  val benfordScoreDist = new BenfordScoreDist(considerZero=false)
  // Numeric column => Error score using chi-squared test
  val benfordScoreChi = new BenfordScoreChi(considerZero=false)
  // Column of Benford counts => Benford counts (collated)
  val benfordCountsSum = new BenfordCountsSum

  // Benford counts => Normalised Benford counts
  val getProbFromCounts = udf((xs: BenfordCountsType) => {
    val (probArr, n) = arrToProbArr(xs, considerZero=false)
    probArr
  })

  // Benford counts => Error score using Cho–Gaines's d
  val getBenfordScoreDistFromCounts = udf[BenfordScoreType, BenfordCountsType]((xs: BenfordCountsType) => arrToScoreDist(xs, considerZero=false) )
  // Benford counts => Error score using chi-squared test
  val getBenfordScoreChiFromCounts = udf[BenfordScoreType, BenfordCountsType]((xs: BenfordCountsType) => arrToScoreChi(xs, considerZero=false) )

}

trait BenfordStuff {

  type BenfordCountsType = Seq[Long]
  val BenfordCountsSparkType: DataType = DataTypes.createArrayType(LongType, false)

  type NormBenfordCountsType = Seq[Double]
  val NormBenfordCountsSparkType: DataType = DataTypes.createArrayType(DoubleType, false)

  type BenfordScoreType = Double
  val BenfordScoreSparkType: DataType = DoubleType

  def getFirstSigDigit(number: Double): Int = {
    if (number == 0)
      return 0
    val num = math.abs(number)
    val exp = math.floor(math.log10(num))
    val ans = num * math.pow(10, -exp)
    math.floor(ans).toInt
  }

  def combineTwoSeqs(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val existingSeq = buffer1.getAs[BenfordCountsType](0)
    val inpSeq = buffer2.getAs[BenfordCountsType](0)
    buffer1(0) = existingSeq.zip(inpSeq).map{x => x._1 + x._2}
  }

  //map and array both have the raw counts - but no more map
  // def mapToArr[T: Numeric](input: Map[Int, T]): Seq[T] = (0 to 9).map(x => input.getOrElse(x, implicitly[Numeric[T]].fromInt(0))).toSeq

  //probability array has the scaled counts
  def arrToProbArr(arr: BenfordCountsType, considerZero: Boolean = false): (NormBenfordCountsType, Long) = {
    // keeping the "prob" for 0 there for reference, but ignoring it when calculating n and rough probability
    val n = {
      if (considerZero)
        arr.sum
      else
        arr.drop(1).sum
    }
    val probArr = arr.map{_.doubleValue / n}
    (probArr, n)
  }

  def expectedProbForDigit(d: Int) = {
    if (d == 0)
      0
    else
      math.log10(1 + 1.0/d)
  }

  val expectedBenfordDist: NormBenfordCountsType = (0 to 9).map(expectedProbForDigit)

  def probArrToScoreDist(input: NormBenfordCountsType, considerZero: Boolean = false): BenfordScoreType = {
    // cho-gaines' distance (d) statistic
    val (modInput: NormBenfordCountsType, offset: Int) = {
      if (considerZero)
        (input, 0)
      else
        (input.drop(1), 1)
    }
    val squaredDiffs = modInput.zipWithIndex.map{ case (obs, i) =>
      val exp = expectedBenfordDist(i + offset)
      math.pow(obs - exp, 2)
    }
    math.sqrt(squaredDiffs.sum)
  }

  def arrToScoreDist(arr: BenfordCountsType, considerZero: Boolean = false): BenfordScoreType = {
    val (probArr, n) = arrToProbArr(arr, considerZero)
    probArrToScoreDist(probArr, considerZero)
  }

  def arrToScoreChi(input: BenfordCountsType, considerZero: Boolean = false): BenfordScoreType = {
    // chi-squared test
    val (modInput: BenfordCountsType, offset: Int) = {
      if (considerZero)
        (input, 0)
      else
        (input.drop(1), 1)
    }
    val n = modInput.sum
    modInput.zipWithIndex.map{ case (obs, i) =>
      val exp = expectedBenfordDist(i + offset) * n
      math.pow(obs - exp, 2) / exp
    }.sum
  }

}

abstract class BenfordLaw extends UserDefinedAggregateFunction with BenfordStuff {

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("frequencySeq", BenfordCountsSparkType) :: Nil
  )

  override def deterministic: Boolean = true

  // This is the initial value for the buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
  }

  // This is how you merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = combineTwoSeqs(buffer1, buffer2)

}

abstract class BenfordLawNumbers extends BenfordLaw {

  // This is the input fields for your aggregate function.
  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inpNum = input.getAs[Double](0)
    val inpDigit = getFirstSigDigit(inpNum)
    val existingBuf = buffer.getAs[BenfordCountsType](0)
    buffer(0) = existingBuf.zipWithIndex.map{ case (count, i) =>
      count + (if (inpDigit == i) 1 else 0)
    }
  }

}

class BenfordCounts extends BenfordLawNumbers {

  // This is the output type of your aggregation function.
  override def dataType: DataType = BenfordCountsSparkType

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): BenfordCountsType = {
    buffer.getAs[BenfordCountsType](0)
  }

}

class BenfordProbs(considerZero: Boolean = false) extends BenfordLawNumbers {

  // This is the output type of your aggregation function.
  override def dataType: DataType = NormBenfordCountsSparkType

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): NormBenfordCountsType = {
    val arr = buffer.getAs[BenfordCountsType](0)
    val (probArr, n) = arrToProbArr(arr, considerZero)
    probArr
  }

}

class BenfordScoreDist(considerZero: Boolean = false) extends BenfordLawNumbers {

  // This is the output type of your aggregation function.
  override def dataType: DataType = BenfordScoreSparkType

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): BenfordScoreType = {
    val arr = buffer.getAs[BenfordCountsType](0)
    arrToScoreDist(arr, considerZero)
  }

}

class BenfordScoreChi(considerZero: Boolean = false) extends BenfordLawNumbers {

  // This is the output type of your aggregation function.
  override def dataType: DataType = BenfordScoreSparkType

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): BenfordScoreType = {
    val arr = buffer.getAs[BenfordCountsType](0)
    arrToScoreChi(arr, considerZero)
  }

}

class BenfordCountsSum extends BenfordLaw {

  // This is the input fields for your aggregate function.
  override def inputSchema: StructType = StructType(StructField("value", BenfordCountsSparkType) :: Nil)

  // This is the output type of your aggregation function.
  override def dataType: DataType = BenfordCountsSparkType

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = combineTwoSeqs(buffer, input)

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): BenfordCountsType = {
    buffer.getAs[BenfordCountsType](0)
  }

}
