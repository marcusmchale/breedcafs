MATCH (: Country {
	name_lower:toLower($country)
})<-[:IS_IN]-(region: Region)
RETURN [
  region.name_lower,
  region.name
]
ORDER BY country.name
