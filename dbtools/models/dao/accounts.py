from dbtools.models.cypher import queries


class UserDAO:

	@staticmethod
	def load_details(tx, user):
		user_record = tx.run(queries['get_user_properties'], username=user.username)
		if not user_record:
			return user
		user.password = user_record['password']
		user.confirmed = user_record['confirmed']
		user.access = user_record['access']
		return user

