package attempt

import ch.hsr.geohash.GeoHash

object GeoHashCodeTest {
  def main(args: Array[String]): Unit = {
    val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(30.228535040660184,115.43071891393716, 12)
    println(geoHashCode)
  }

}
