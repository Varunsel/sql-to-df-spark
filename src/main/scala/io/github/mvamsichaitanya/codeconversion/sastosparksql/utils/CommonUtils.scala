package io.github.mvamsichaitanya.codeconversion.sastosparksql.utils

class CommonUtils {


  /**
    * Function to replace exact sub string even if replace string contains regex specific char's
    *
    * @param s        String
    * @param replace  String that need to be replaced
    * @param replaced replaced string
    * @return
    */
  def replace(s: String, replace: String, replaced: String): String = {

    s.foreach {
      c => {

        val firstChar = replace.head
        if(c==firstChar){

        }
      }
    }

    ???
  }
}
