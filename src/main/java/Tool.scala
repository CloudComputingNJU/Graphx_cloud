import java.nio.ByteBuffer

/**
  * author: Qiao Hongbo
  * time: {$time}
  **/
object Tool {
  def utf8ToLong(singleCharacter: String):Long={
    var bytes : Array[Byte] = {Byte,0,0,0,0,0,0,0}
    ByteBuffer.wrap(singleCharacter.getBytes("utf-8"))
  }
}
