MATCH
  (:Country {name_lower:toLower($country)})
    <-[:IS_IN]-(:Region)
    <-[:IS_IN]-(:Farm)
    <-[:IS_IN*]-(tree: Tree)
RETURN count(distinct(tree))