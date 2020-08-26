MATCH
  (user: User {
    email: toLower(trim($email))
  })
SET
  user.confirmed = true