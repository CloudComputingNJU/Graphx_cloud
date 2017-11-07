import java.nio.ByteBuffer

/**
  * author: Qiao Hongbo
  * time: {$time}
  **/
object Tool {
  def utf8ToLong(singleCharacter: String): Long = {
    var bytes: Array[Byte] = new Array[Byte](8)
    val characterBytes = singleCharacter.getBytes("utf-8")
    var i = characterBytes.length-1
    while(i>0){
      bytes(7-i) = characterBytes(i)
      i = i - 1
    }
    ByteBuffer.wrap(bytes).getLong
  }

  def main(args: Array[String]): Unit = {
    val figure: Long = utf8ToLong("一")
    val figure2: Long = utf8ToLong("二")

    println(figure)
    println(figure2)
  }
}
