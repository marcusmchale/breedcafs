//first get all the data collections and link to a base node formed from field
MATCH
  (:User {username_lower: toLower($username)})
  -[:SUBMITTED*3]->(uff:UserFieldInput)
  -[s:SUBMITTED]->(record: Record)
  -[:RECORD_FOR]->(),
  (uff)-[:CONTRIBUTED]->(ff:FieldInput)
  -[:FROM_FIELD | FOR_ITEM]->(field: Field),
  (ff)-[:FOR_INPUT]->(input: Input)
WHERE s.time >= $starttime AND s.time <= $endtime
WITH
  input, count(record) as record_count, field
RETURN
  "Input" as d_label,
  input.name + " (" + toString(record_count) + ")" as d_name,
  id(field) + "_" + id(input) as d_id,
  "Field" as n_label,
  field.name as n_name,
  id(field) as n_id,
  "FROM" as r_type,
  id(field) + "_" + id(input) + "_rel" as r_id,
  id(field) + "_" + id(input) as r_start,
  id(field) as r_end
UNION
//get users farm context
MATCH
  (:User {username_lower: toLower($username)})
  -[:SUBMITTED*3]->(:UserFieldInput)
  -[:CONTRIBUTED]->(: FieldInput)
  -[:FOR_ITEM | FROM_FIELD]->(field:Field)
  -[:IS_IN]->(farm:Farm)
RETURN
  "Field" as d_label,
  field.name as d_name,
  id(field) as d_id,
  "Farm" as n_label,
  farm.name as n_name,
  id(farm) as n_id,
  "IS_IN" as r_type,
  (id(field) + "_" + id(farm)) as r_id,
  id(field) as r_start,
  id(farm) as r_end
UNION
//link the above into region context
MATCH
  (:User {username_lower: toLower($username)})
  -[:SUBMITTED*3]->(:UserFieldInput)
  -[:CONTRIBUTED]->(: FieldInput)
  -[:FOR_ITEM | FROM_FIELD]->(:Field)
  -[:IS_IN]->(farm: Farm)
  -[:IS_IN]->(region: Region)
RETURN
  "Farm" as d_label,
  farm.name as d_name,
  id(farm) as d_id,
  "Region" as n_label,
  region.name as n_name,
  id(region) as n_id,
  "IS_IN" as r_type,
  (id(farm) + "_" + id(region)) as r_id,
  id(farm) as r_start,
  id(region) as r_end
UNION
//link the above into country context
MATCH
  (:User {username_lower: toLower($username)})
  -[:SUBMITTED*3]->(:UserFieldInput)
  -[:CONTRIBUTED]->(: FieldInput)
  -[:FOR_ITEM | FROM_FIELD]->(: Field)
  -[:IS_IN]->(: Farm)
  -[:IS_IN]->(region: Region)
  -[:IS_IN]->(country: Country)
RETURN
  "Region" as d_label,
  region.name as d_name,
  id(region) as d_id,
  "Country" as n_label,
  country.name as n_name,
  id(country) as n_id,
  "IS_IN" as r_type,
  (id(region) + "_" + id(country)) as r_id,
  id(region) as r_start,
  id(country) as r_end
