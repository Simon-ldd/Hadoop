package com.simon.scala1

import java.io.{BufferedReader, File, FileReader}

class RichFile(file:File) {

  def count():Int = {
    // 读取数据
    val fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)

    var sum = 0
    try {
      while(bufferedReader.readLine() != null) {
        sum += 1
      }
    } catch {
      case _:Exception => sum
    } finally {
      fileReader.close()
      bufferedReader.close()
    }

    sum
  }
}
