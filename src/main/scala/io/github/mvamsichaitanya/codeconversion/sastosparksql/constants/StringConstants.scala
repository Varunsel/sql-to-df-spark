package io.github.mvamsichaitanya.codeconversion.sastosparksql.constants

object StringConstants {

  final val TranslateRegex = "Regex"

  final val TranslateFunctionException =
    "translate function cannot be processed " +
      "due more than three elements found"

  final val TableNamePrefix = ""

  final val TripleQuote ="""""""" + """""""

  final val EndFunctionIndicators = Seq[Char](' ',')',',')

}
