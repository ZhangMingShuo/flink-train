object scala_test {
  def main(args: Array[String]): Unit = {
    val sparkPath = "https://www.bilibili.com/video/av62719073/?p="
    for(a <- 81 to 94)
      println(sparkPath+a)
  }
}