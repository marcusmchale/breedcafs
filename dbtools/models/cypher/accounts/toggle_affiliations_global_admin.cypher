UNWIND
  $username_partner_list as username_partner
  MATCH
    (p: Partner {
      name_lower: toLower(username_partner[1])
    })<-[a: AFFILIATED]-(u: User {
      username_lower: toLower(username_partner[0])
    })
  SET
    a.confirmed = NOT a.confirmed,
    a.confirm_timestamp = a.confirm_timestamp +  datetime.transaction().epochMillis