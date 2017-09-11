#add submission time for all SUBMITTED with ON MATCH SET and ON CREATE SET

class Cypher():
	user_find = ('MATCH (user:User) '
	' WHERE user.username = $username '
	' OR user.email = $email '
	' RETURN user ')
	username_find = ('MATCH (user:User) '
		' WHERE user.username = $username '
		' RETURN user ')
	email_find = ('MATCH (user:User) '
		'WHERE user.email = $email '
		'RETURN user ')
	confirm_email = ('MATCH (user:User) '
		' WHERE user.email = $email '
		' SET user.confirmed = "True" ')
	password_reset = ('MATCH (user:User) '
		' WHERE user.email = $email '
		' SET user.password = $password ')
	user_register = ('MATCH (partner:Partner) '
		' WHERE partner.name = $partner '
		' CREATE (user:User {username : $username, '
		' password : $password, email : $email, name : $name,'
		' confirmed : $confirmed}) '
		' -[r:Affiliated] -> (partner) ')
	user_del = ('MATCH (user:User) '
		' WHERE user.username = $username '
		' OR user.email = $email '
		' DELETE user ')
	upload_submit = (' LOAD CSV WITH HEADERS FROM $filename as csvLine '
		' MATCH (u:User {username : $username}), '
		' (t:Tree {uid:csvLine.UID}), '
		' (tr:Trait {name:csvLine.trait}) '
		' MERGE (t)<-[:DATA_FROM]-(d:Data {value:csvLine.value,'
		' timeFB:csvLine.timestamp,'
		' person:csvLine.person, '
		' location:csvLine.location}) '
		' -[:DATA_FOR]->(tr)'
		' ON MATCH SET d.found = "TRUE" '
		' ON CREATE SET d.found = "FALSE" '
		' CREATE (u)-[:SUBMITTED {timeInt : timestamp(), '
		' submission_type : $submission_type }]'
		' ->(d)'
		' RETURN d.found')
	country_find = ('MATCH (country:Country {name : $country}) '
		' RETURN country ')
	country_add = ('MATCH (user:User {username:$username}) '
		' MERGE (country:Country {name :$country}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	region_find = ('MATCH (:Country {name : $country}) '
		'<-[:IS_IN]-(region:Region { name:$region}) '
		' RETURN region ')
	region_add = ('MATCH (user:User {username:$username}), '
		' (c:Country {name : $country}) '
		' MERGE (c)<-[:IS_IN]- '
		' (:Region { name:$region}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)')
	farm_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(farm:Farm { name: $farm}) '
	 	' RETURN farm ')
	farm_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(r:Region {name : $region}) '
		' MERGE (r) <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)')
	plot_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(:Farm { name: $farm}) '
	 	' <-[:IS_IN]-(plot:Plot { name: $plot}) '
	 	' RETURN plot.name as name')
#for autoincrement:
# https://stackoverflow.com/questions/32040409/reliable-autoincrementing-identifiers-for-all-nodes-relationships-in-neo4j
# for lock:
#this allows the increment counter (allowing for concurrent transactions to be serialised):
#http://neo4j.com/docs/stable/transactions-isolation.html
#https://stackoverflow.com/questions/35138645/how-to-perform-an-atomic-update-on-relationship-properties-with-py2neo
#https://stackoverflow.com/questions/31798311/write-lock-behavior-in-neo4j-cypher-over-transational-rest-ap
#This was tested by running many threads on the same operation (plot_add)
#without plot_id_lock (as a separate cypher query) it fails to maintain the count properly and clashes with the unique UID constraint
	plot_id_lock = (' MERGE (id:PlotID{name:"Plots"}) '
		' ON CREATE SET id._LOCK_ = true, id.count=0 '
		' ON MATCH SET id._LOCK_ = true ')
	plot_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country})<-[:IS_IN]-(:Region {name : $region}) '
		' <-[:IS_IN]-(f:Farm { name: $farm}) '
		' MATCH (id:PlotID{name:"Plots"}) '
		' SET id.count=id.count+1 '
		' MERGE (f)<-[:IS_IN]-(:Plot { name:$plot, uid:id.count}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) '
		' SET id._LOCK_ = false')
	tree_id_lock = (' MATCH  (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region {name : $region}) ' 
		' <-[:IS_IN]-(:Farm {name: $farm}) '
		' <-[:IS_IN]-(p:Plot {name: $plot})'
		' MERGE (p)<-[:ID_COUNTER_FOR]-(id:TreeID{plotID:p.uid})' 
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ')
	trees_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region {name : $region}) ' 
		' <-[:IS_IN]-(:Farm { name: $farm}) '
		' <-[:IS_IN]-(p:Plot {name: $plot})' 
		' UNWIND range(1, toInt($count)) as counter ' 
		' MATCH (id:TreeID{plotID:p.uid})' 
		' SET id.count=id.count+1'
		' MERGE (p)<-[:IS_IN]-(t:Tree {uid:(p.uid + "_" + id.count)}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)'
		' SET id._LOCK_ = false '
		' RETURN [t.uid, p.uid, id.count]')
	trees_get = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region {name : $region}) ' 
		' <-[:IS_IN]-(:Farm { name: $farm}) '
		' <-[:IS_IN]-(p:Plot {name: $plot})' 
		' <-[:IS_IN]-(t:Tree)'
		' WHERE toInt(last(split(t.uid, "_")))>=$start '
		' AND toInt(last(split(t.uid, "_")))<=$end '
		' RETURN [t.uid, p.uid, last(split(t.uid, "_"))]')
	get_farms = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(f:Farm) '
		' RETURN properties (f)')
	get_plots = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:IS_IN]-(p:Plot) '
		' RETURN properties (p)')
	get_submissions_range = ('MATCH (:User {username:$username})'
		' -[s:SUBMITTED]->(d)'
		' WHERE s.timeInt>=$starttimeInt AND s.timeInt<=$endtimeInt'
		' MATCH (d)-[r]-(n)'
		' WHERE NOT type (r) IN ["SUBMITTED","ID_COUNTER_FOR"] '
		' RETURN '
			' labels(d)[0] as d_label, '
			' coalesce(d.name,d.uid,d.value) as d_name,'
			' id(d) as d_id, '
			' labels(n)[0] as n_label, '
			' coalesce(n.name,n.uid,n.value) as n_name,'
			' id(n) as n_id, '
			' id(r) as r_id, '
			' type(r) as r_type, '
			' id(startNode(r)) as r_start, '
			' id(endNode(r)) as r_end '
		'LIMIT 100')
	get_plots_treecount = (' '
		' MATCH (C:Country) '
		' OPTIONAL MATCH (C) <-[a:IS_IN]-(R:Region) '
		' OPTIONAL MATCH (R) <-[b:IS_IN]-(F:Farm) '
		' OPTIONAL MATCH (F) <-[c:IS_IN]-(P:Plot) '
		' OPTIONAL MATCH (P) <-[d:ID_COUNTER_FOR]-(T:TreeID) '
		' RETURN '
			' labels(C)[0] as C_label, '
			' labels(R)[0] as R_label, '
			' labels(F)[0] as F_label, '
			' labels(P)[0] as P_label, '
			' C.name, R.name, F.name, P.name, '
			' P.uid, T.count ')
