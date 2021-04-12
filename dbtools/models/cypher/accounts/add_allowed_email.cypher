MATCH
  (:User {
    username_lower: toLower($username)
  })
  -[:SUBMITTED]->(: Submissions)
  -[:SUBMITTED]->(e: Emails)
SET e.allowed = e.allowed + [toLower(trim($email))]