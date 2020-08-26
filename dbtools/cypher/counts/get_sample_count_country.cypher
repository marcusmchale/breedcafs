MATCH
  (:Country {name_lower:toLower($country)})
    <-[:IS_IN]-(:Region)
    <-[:IS_IN]-(:Farm)
    <-[:IS_IN | FROM*]-(sample: Sample)
RETURN count(distinct(sample))