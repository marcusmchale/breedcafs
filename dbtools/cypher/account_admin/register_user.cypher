MATCH
  (partner: Partner {
    name_lower: toLower($partner)
  })
CREATE
  (user: User {
    username_lower: toLower($username),
    username: trim($username),
    password: $password,
    email: toLower($email),
    name: $name,
    time: datetime.transaction().epochMillis,
    access: ["user"],
    confirmed: false,
  })-[: AFFILIATED {
    data_shared: true,
    confirmed: false,
    confirm_timestamp: [],
    admin: false
  }]->(partner),
  (user)-[: SUBMITTED]->(sub: Submissions),
  (sub)-[: SUBMITTED]->(: Emails {allowed :[]}),
  (sub)-[: SUBMITTED]->(locations: Locations),
  (locations)-[: SUBMITTED]->(: Countries),
  (locations)-[: SUBMITTED]->(: Regions),
  (locations)-[: SUBMITTED]->(: Farms),
  (sub)-[:SUBMITTED]->(items: Items),
  (items)-[: SUBMITTED]->(: Fields),
  (items)-[: SUBMITTED]->(: Blocks),
  (items)-[: SUBMITTED]->(: Trees),
  (items)-[: SUBMITTED]->(: Samples),
  (sub)-[:SUBMITTED]->(: Records)
