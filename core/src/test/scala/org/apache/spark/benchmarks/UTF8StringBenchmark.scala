/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.benchmarks

import scala.util.Random

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Benchmark

object UTF8StringBenchmark {

  def stringComparisons(iters: Long): Unit = {
    val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    val random = new Random(0)

    def randomString(min: Int, max: Int): String = {
      val len = random.nextInt(max - min) + min
      val sb = new StringBuilder(len)
      var i = 0
      while (i < len) {
        sb.append(chars.charAt(random.nextInt(chars.length())))
        i += 1
      }
      sb.toString
    }

    val count = 16 * 1000

    val dataTiny = Seq.fill(count)(randomString(2, 7))
      .map(UTF8String.fromString).toArray

    val dataSmall = Seq.fill(count)(randomString(8, 16))
      .map(UTF8String.fromString).toArray

    val dataMedium = Seq.fill(count)(randomString(16, 32))
      .map(UTF8String.fromString).toArray

    val dataLarge = Seq.fill(count)(randomString(512, 1024))
      .map(UTF8String.fromString).toArray

    def strings(data: Array[UTF8String]) = { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        i = 0
        while (i < count) {
          sum += data(i).compareTo(data((i + 1) % count))
          i += 1
        }
      }
    }

    val benchmark = new Benchmark("String compareTo", count * iters, 25)
    benchmark.addCase("2-7 byte")(strings(dataTiny))
    benchmark.addCase("8-16 byte")(strings(dataSmall))
    benchmark.addCase("16-32 byte")(strings(dataMedium))
    benchmark.addCase("512-1024 byte")(strings(dataLarge))
    benchmark.run
  }

  def main(args: Array[String]): Unit = {
    stringComparisons(1024 * 4)
  }
}
