MATCH
  (u: User)-[a: AFFILIATED]->(p: Partner)
RETURN {
  Username : u.username,
  Email : u.email,
  Name : u.name,
  Partner : p.name,
  PartnerFullName : p.fullname,
  Confirmed : a.admin
}