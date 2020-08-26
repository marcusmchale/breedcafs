MATCH
	(user: User {
		username_lower: toLower($username)
	})
UNWIND
  $username_partner_list as username_partner
  MATCH
		(user)-[: AFFILIATED {
			admin : true
		}]->(p: Partner {
			name_lower:username_partner[1]
		})<-[a: AFFILIATED]-(: User {
      username_lower: toLower(username_partner[0])
    })
  SET
    a.confirmed = NOT a.confirmed,
    a.confirm_timestamp = a.confirm_timestamp +  datetime.transaction().epochMillis
