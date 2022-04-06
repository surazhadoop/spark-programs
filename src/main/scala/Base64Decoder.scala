object Base64Decoder {
  def main(args: Array[String]): Unit = {
    import java.util.Base64

    val encoded = "cmZAeXNnLmNvcg=="
    val decoded = new String(Base64.getDecoder().decode(encoded))

    println(decoded)
  }
}
