UNWIND
  $partners as partner
  MATCH
    (u: User {
    username_lower: toLower($username)
    })
  MATCH
    (p: Partner {
      name_lower: toLower(partner)
    })
  MERGE
    (u)-[a: AFFILIATED]->(p)
  ON CREATE SET
    a.add_timestamp = datetime.transaction().epochMillis,
    a.data_shared =  false,
    a.admin = false,
    a.confirm_timestamp = [],
    a.confirmed = false