MATCH (: Country {
  name_lower: toLower($country)
})<-[:IS_IN]-(: Region {
  name_lower: toLower($region)
})<-[:IS_IN]-(farm: Farm)
RETURN [
  farm.name_lower,
  farm.name
]
ORDER BY farm.name