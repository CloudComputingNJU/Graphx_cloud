import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]){
      println("yes")
  }

  def getCharacterPair(content: String): Array[Array[String]] = {
      var characterArray = content.split("")
      var edgeArray = new ArrayBuffer[Array[String]]()
      var i = 0
      for (i <- 0 until characterArray.length-1){
//          edgeArray += Array[String](characterArray(i), characterArray(i+1))
          edgeArray += null
//          var edgeArrayD = Array[String](characterArray(i), characterArray(i+1))
//          var edgeArrayD = Array[String]("123", "543")
      }
      edgeArray.toArray[Array[String]]
//      characterArray
//    null
    }
}
