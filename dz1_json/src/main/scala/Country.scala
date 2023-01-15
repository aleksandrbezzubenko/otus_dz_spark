//case class Awg (
//                 name: String,
//                 symbol: String
//               )
//
//case class Currencies (
//                        AWG: Awg
//                      )
//
//case class Demonyms (
//                      eng: Eng,
//                      fra: Eng
//                    )
//
//case class Eng (
//                 f: String,
//                 m: String
//               )
//
//case class Idd (
//                 root: String,
//                 suffixes: Seq[String]
//               )
//
//case class Name (
//                  common: String,
//                  official: String,
//                  native: Native
//                )
//
//case class Native (
//                    nld: String,
//                    pap: String
//                  )
//
//case class Nld (
//                 official: String,
//                 common: String
//               )
//
//case class RootInterface (
//                           name: Name,
//                           tld: Seq[String],
//                           cca2: String,
//                           ccn3: String,
//                           cca3: String,
//                           cioc: String,
//                           independent: Boolean,
//                           status: String,
//                           unMember: Boolean,
//                           currencies: Currencies,
//                           idd: Idd,
//                           capital: Seq[String],
//                           altSpellings: Seq[String],
//                           region: String,
//                           subregion: String,
//                           languages: Native,
//                           translations: Translations,
//                           latlng: Seq[Double],
//                           landlocked: Boolean,
//                           borders: Seq[String],
//                           area: Int,
//                           flag: String,
//                           demonyms: Demonyms
//                         )
//
//case class Translations (
//                          ces: Nld,
//                          deu: Nld,
//                          est: Nld,
//                          fin: Nld,
//                          fra: Nld,
//                          hrv: Nld,
//                          hun: Nld,
//                          ita: Nld,
//                          jpn: Nld,
//                          kor: Nld,
////                          nld: Nld,
//                          per: Nld,
//                          pol: Nld,
//                          por: Nld,
//                          rus: Nld,
//                          slk: Nld,
//                          spa: Nld,
//                          swe: Nld,
//                          urd: Nld,
//                          zho: Nld
//                        )


case class Country (
    name: Name,
    capital: Seq[String],
    region: String,
    area: Int
                   )
