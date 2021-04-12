MATCH (region: Region)
RETURN [
  region.name_lower,
  region.name
]
ORDER BY country.name