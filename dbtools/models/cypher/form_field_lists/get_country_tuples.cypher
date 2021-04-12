MATCH (country:Country)
RETURN [
  country.name_lower,
  country.name
]
ORDER BY country.name