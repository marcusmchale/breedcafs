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
			' password : $password, ' 
			' email : $email, '
			' name : $name, '
			' time : timestamp(), '
			' confirmed : $confirmed}) '
		' -[r:Affiliated] -> (partner) ')
	user_del = ('MATCH (user:User) '
		' WHERE user.username = $username '
		' OR user.email = $email '
		' DELETE user ')
	upload_submit =	( ' MATCH (user:User {username : $username}) '
		#so that users don't have a relationship for each submission
		' MERGE (user)-[:SUBMITTED_DATA_TYPE]->(u:FieldBookSubmissions)'
		' WITH u '
		# Now we keep that node and load in the csv
		' LOAD CSV WITH HEADERS FROM $filename as csvLine '
		# And identify the plots and traits assessed
		' MATCH (plot:Plot {uid:toInt(head(split(csvLine.UID, "_")))}), '
			' (tree:Tree {uid:csvLine.UID}),'
			' (trait:Trait {name:csvLine.trait}) '
		# Data is split out to container node per plot per trait level
		# once again to reduce the number of relationships when later searching
		' MERGE (plot)-[:TRAIT_DATA {trait:csvLine.trait}]-> '
			' (pts:PlotTraitData {trait:csvLine.trait, plotID:toInt(head(split(csvLine.UID, "_")))}) '
			' <-[:PLOT_DATA {plotID:toInt(head(split(csvLine.UID, "_")))}]-(trait) '
		#Then create only unique data points off these containers then link to tree
		#If any of tree, value, timeFB, person or location are different, a new node is created
		' MERGE (pts)-[s:SUBMISSIONS]-> '
			' (d:Data {tree:csvLine.UID, '
				' value:csvLine.value, '
				' timeFB:csvLine.timestamp, '
				' person:csvLine.person, '
				' location:csvLine.location}) '
				' -[:DATA_FOR]->(tree)'
			' ON MATCH SET d.found="TRUE" '
			#recording details of first submission
			' ON CREATE SET d.found="FALSE", s.username=$username, s.timestamp=timestamp()'
		# but to allow faster identification of a users submissions we store attempts to update a PlotTrait node
		' MERGE (u)-[:UPDATED {time:timestamp()}]-(pts)'
		#And give the user feedback on their submission success
		' RETURN d.found' )
	country_find = ('MATCH (country:Country {name : $country}) '
		' RETURN country ')
	country_add = ('MATCH (user:User {username:$username}) '
		' MERGE (country:Country {name :$country}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user) ' )
	region_find = ('MATCH (:Country {name : $country}) '
		'<-[:IS_IN]-(region:Region { name:$region}) '
		' RETURN region ')
	region_add = ('MATCH (user:User {username:$username}), '
		' (c:Country {name : $country}) '
		' MERGE (c)<-[:IS_IN]- '
		' (:Region { name:$region}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user)')
	farm_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(farm:Farm { name: $farm}) '
	 	' RETURN farm ')
	farm_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(r:Region {name : $region}) '
		' MERGE (r) <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user)')
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
		' <-[:SUBMITTED {time : timestamp()}]-(user) '
		' SET id._LOCK_ = false')
	tree_id_lock = (' MATCH (p:Plot {uid: $plotID})'
		' MERGE (p)-[:CONTAINS_TREES]->(:PlotTrees) '
		' <-[:COUNTER_FOR]-(id:TreeCounter)'
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ')
	trees_add = ('MATCH (user:User {username:$username}) '
		' MERGE (user)-[:SUBMITTED_TREES]->(u:TreeSubmissions) '
		' WITH u'
		' MATCH	(c:Country)<-[:IS_IN]-(r:Region)<-[:IS_IN]-(f:Farm) '
		' <-[:IS_IN]-(p:Plot {uid: $plotID}) '
		' -[:CONTAINS_TREES]->(pt:PlotTrees) '
		' <-[:COUNTER_FOR]-(id:TreeCounter) '
		' UNWIND range(1, toInt($count)) as counter ' 
		' SET id.count=id.count+1 '
		' MERGE (pt)-[:REGISTERED_TREE { '
				' username:$username, '
				' timestamp:timestamp()}]'
			'->(t:Tree {uid:(p.uid + "_" + id.count), '
			' id:id.count}) '
		' MERGE (u)-[:UPDATED {time:timestamp()}]-(pt)'
		' SET id._LOCK_ = false '
		' RETURN [t.uid, p.uid, t.id, p.name, f.name, r.name, c.name]'
		' ORDER BY t.id')
	trees_get = ('MATCH (c:Country)<-[:IS_IN]'
		' -(r:Region)<-[:IS_IN]-(f:Farm)<-[:IS_IN]-'
		' (p:Plot {uid: $plotID})-[:CONTAINS_TREES]->(pt:PlotTrees) '
		' -[:REGISTERED_TREE]->(t:Tree)'
		' WHERE t.id>=$start '
		' AND t.id<=$end '
		' RETURN [t.uid, p.uid, t.id, p.name, f.name, r.name, c.name]'
		' ORDER BY t.id')
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
		' WHERE s.time>=$starttime AND s.time<=$endtime'
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
		' OPTIONAL MATCH (C) <-[:IS_IN]-(R:Region) '
		' OPTIONAL MATCH (R) <-[:IS_IN]-(F:Farm) '
		' OPTIONAL MATCH (F) <-[:IS_IN]-(P:Plot) '
		' OPTIONAL MATCH (P) -[:CONTAINS_TREES]->(pt:PlotTrees) '
		' OPTIONAL MATCH (pt) <-[:COUNTER_FOR]-(T:TreeCounter) '
		' RETURN '
			' labels(C)[0] as C_label, '
			' labels(R)[0] as R_label, '
			' labels(F)[0] as F_label, '
			' labels(P)[0] as P_label, '
			' C.name, R.name, F.name, P.name, P.uid, T.count ' )
	tissue_add = ('MATCH (user:User {username:$username}) '
		' MERGE (tissue:Tissue {name :$tissue}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user) ' )
	storage_add = ('MATCH (user:User {username:$username}) '
		' MERGE (storage:Storage {name :$storage}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user) ' )
	sample_id_lock = (' MATCH  (p:Plot {uid: $plotID})'
		' MERGE (p)<-[:SAMPLES_FROM]-(:PlotSamples) '
		' <-[:ID_COUNTER_FOR]-(id:SampleID)'
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ')
	samples_add = (' MATCH (p:Plot {uid: $plotID}) '
		' -[:CONTAINS_TREES]->(pt:PlotTrees) '
		' -[:REGISTERED_TREE]-(t:Tree) '
		#with the selected trees to be sampled
		' WITH t '
		' ORDER BY t.id '
		' WHERE t.id >= $start '
		' AND t.id <= $end '
		' MATCH'
			' (user:User {username:$username}), '
			' (tissue:Tissue {name:$tissue}), '
			' (storage:Storage {name:$storage}), '
			' (c:Country)<-[:IS_IN]-(r:Region)<-[:IS_IN]-(f:Farm) '
				' <-[:IS_IN]-(p:Plot {uid:$plotID})<-[:SAMPLES_FROM]-(ps:PlotSamples) '
				' <-[:ID_COUNTER_FOR]-(id:SampleID )'
		#with $replicates
		' UNWIND range(1, $replicates) as replicates '
		#store the sample linked to its tree and plot incrementing with a plot level counter
		' SET id.count=id.count+1 '
		' MERGE (t)<-[:SAMPLE_FROM_TREE] '
		' -(s:Sample {uid:(t.uid + "_" + id.count),'
			' id:id.count, date:$date, tissue:$tissue, storage:$storage}) '
		' -[:SAMPLE_FROM_PLOT]->(ps)'
		#track user submissions
		' MERGE (user)-[:SUBMITTED_SAMPLES]-(ss:SampleSubmissions) '
		' MERGE (ss)-[:UPDATED {time:timestamp()}]->(ps) '
		#track tissue types and storage methods from plot
		' MERGE (tissue)-[:COLLECTED_AT]->(ps) '
		' MERGE (storage)-[:STORED_AT]->(ps) '
		#return for csv
		' RETURN [s.uid, p.uid, t.id, s.id, s.date, tissue.name, storage.name, p.name, f.name, r.name, c.name] '
		' ORDER BY s.id')
	soil_add = ('MATCH (user:User {username:$username}) '
		' MERGE (:Soil {name :$soil}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user) ' )
	shade_tree_add = ('MATCH (user:User {username:$username}) '
		' MERGE (:ShadeTree {name :$shade_tree}) '
		' <-[:SUBMITTED {time : timestamp()}]-(user) ' )
	field_details_add = ('MATCH (user:User {username:$username}), '
		' (p:Plot {plotID:$plotID}) '
		#create if necessary container for plot details
		' MERGE (p)-[:DETAIL_SUBMISSIONS]-(pd:PlotDetails)'
		#and user container plot detail submissions
		' MERGE (user)-[:SUBMITTED_PLOT_DETAILS]-(pds:PlotDetailSubmissions)'
		#then log the user update 
		' MERGE (pds)-[:UPDATED {time:timestamp()}]->(pd)'
		#and store the details with a timestamp
		' MERGE (pd)<-[:PLOT_DETAIL]-(:Soil {name:$soil, time:timestamp()})')