MATCH
  (: Country {
    name_lower: toLower($country)
  })<-[:IS_IN]-(: Region)
  <-[:IS_IN]-(farm: Farm)
RETURN [
  farm.name_lower,
  farm.name
]
ORDER BY farm.name