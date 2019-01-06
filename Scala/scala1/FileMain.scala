package com.simon.scala1

import java.io.File

object FileMain {

  // 定义隐式转换
  implicit def file2RichFile(file:File) = new RichFile(file)

  def main(args: Array[String]): Unit = {
    // 1. 加载文件
    val file = new File("/Users/simon/downloads/itstar.log")

    // 2. 打印条数
    println(file.count())
  }
}
