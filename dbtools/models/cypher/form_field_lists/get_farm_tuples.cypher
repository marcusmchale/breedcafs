MATCH (farm: Farm)
RETURN [
  farm.name_lower,
  farm.name
]
ORDER BY farm.name