UNWIND
  $partners as partner
  MATCH
    (u: User {
      username_lower: toLower($username)
    })-[a: AFFILIATED {
      data_shared: false,
      // cant remove primary "data shared" affiliation
      confirm_timestamp: []
      // cant remove an affiliation that has been confirmed at any time
      // this is required to preserve history of access
    }]->(p: Partner {
      name_lower: toLower(partner)
    })
  DELETE a