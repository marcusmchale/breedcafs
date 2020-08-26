//Due to a bug (https://github.com/neo4j/neo4j/issues/12149) list expansion isn't working in where clause,
// A temporary workaround is to modify the list first
WITH
  [c in $countries | toLower(c)] as country_list
MATCH
  (country: Country)
    <-[:IS_IN]-(:Region)
    <-[:IS_IN]-(:Farm)
    <-[:IS_IN *]-(tree: Tree)
//WHERE country.name_lower IN [i in $country_list | toLower(i)]
WHERE country.name_lower IN country_list
RETURN count(distinct(tree))