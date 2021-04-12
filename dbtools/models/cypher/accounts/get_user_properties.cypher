MATCH
  (user: User {username_lower: toLower($username)})
RETURN
  {
    name: user.name,
    email: user.email,
    confirmed: user.confirmed,
    access: user.access,
    time: user.time
  }