MATCH
  (:User {
    username_lower: toLower(trim($username))
  })
  -[:SUBMITTED]->(: Submissions)
  -[:SUBMITTED]->(e: Emails)
SET e.allowed = [x in e.allowed WHERE x <> toLower($email)]
