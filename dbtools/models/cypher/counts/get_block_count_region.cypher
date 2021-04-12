MATCH
  (:Country {name_lower:toLower($country)})
    <-[:IS_IN]-(:Region {name_lower:toLower($region)})
    <-[:IS_IN]-(:Farm)
    <-[:IS_IN*]-(block: Block)
RETURN count(distinct(block))