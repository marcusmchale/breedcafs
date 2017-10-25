class Cypher():
#user procedures
	user_find = ('MATCH (user:User) '
	' WHERE user.username = $username '
	' OR user.email = $email '
	' RETURN user ')
	username_find = ('MATCH (user:User {username : $username}) '
		' RETURN user ')
	email_find = ('MATCH (user:User {email : $email} '
		'RETURN user ')
	confirm_email = ('MATCH (user:User {email : $email}) '
		' SET user.confirmed = true ')
	password_reset = ('MATCH (user:User {email = $email}) '
		' SET user.password = $password ')
	user_register = ('MATCH (partner:Partner {name:$partner}) '
		' CREATE ' 
			' (user:User {username : $username, '
					' password : $password, ' 
					' email : $email, '
					' name : $name, '
					' time : timestamp(), '
					' confirmed : false}) '
				' -[r:AFFILIATED] -> (partner), '
			' (user)-[:SUBMITTED]->(sub:Submissions), '
			' (sub)-[:SUBMITTED]->(locations:Locations), '
				' (locations)-[:SUBMITTED]->(:Countries), '
				' (locations)-[:SUBMITTED]->(:Regions), '
				' (locations)-[:SUBMITTED]->(:Farms), '
				' (locations)-[:SUBMITTED]->(:Plots), '
			' (sub)-[:SUBMITTED]->(items:Items), '
				' (items)-[:SUBMITTED]->(:Blocks), '
				' (items)-[:SUBMITTED]->(:Trees), '
				' (items)-[:SUBMITTED]->(:Samples), '
			' (sub)-[:SUBMITTED]->(data:DataSub), '
				'(data)-[:SUBMITTED]->(:FieldBook)'
			' ')
	user_del = ( ' MATCH (u:User {email:$email, confirmed: false}) '
		' OPTIONAL MATCH (u)-[:SUBMITTED*]->(n) '
		' DETACH DELETE u,n ' )
##LOCATION PROCEDURES
#country procedures
	country_find = ( 'MATCH (country:Country {name : $country}) '
		' RETURN country ' )
	country_add = ( 'MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Locations) '
			' -[:SUBMITTED]->(c:Countries) '
		' MERGE (c)-[s:SUBMITTED]->(:Country {name :$country}) '
			' ON CREATE SET s.time = timestamp() ' )
#region procedures
	region_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(region:Region { name:$region}) '
		' RETURN region ')
	region_add = ( 'MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Locations) '
			' -[:SUBMITTED]->(r:Regions), ' 
		' (c:Country {name : $country}) '
		' MERGE (r)-[s:SUBMITTED]->(:Region { name:$region})-[:IS_IN]->(c) '
			' ON CREATE SET s.time = timestamp() ' )
	get_farms = ('MATCH (f:Farm) '
		' -[:IS_IN]->(:Region {name: $region}) '
		' -[:IS_IN]->(:Country {name: $country}) '
		' RETURN properties (f)')
	farm_find = ( ' MATCH (farm:Farm { name: $farm}) ' 
		' -[:IS_IN]->(:Region { name: $region }) '
		' -[:IS_IN]->(:Country { name: $country }) '
		' RETURN farm ' )
	farm_add = ( 'MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Locations) '
			' -[:SUBMITTED]->(f:Farms), ' 
		' (r:Region {name : $region})-[:IS_IN]->(:Country {name : $country}) '
		' MERGE (f)-[s:SUBMITTED]->(:Farm { name:$farm})-[:IS_IN]->(r) '
			' ON CREATE SET s.time = timestamp() ' )
#plot procedures
	get_plots = ('MATCH (p:Plot) '
		' -[:IS_IN]->(:Farm {name: $farm}) '
		' -[:IS_IN]->(:Region {name: $region}) '
		' -[:IS_IN]->(:Country {name: $country}) '
		' RETURN properties (p)')
	plot_find = ( ' MATCH (plot:Plot { name: $plot}) '
			' -[:IS_IN]->(:Farm { name: $farm}) ' 
			' -[:IS_IN]->(:Region { name: $region }) '
			' -[:IS_IN]->(:Country { name: $country }) ' 
	 	' RETURN plot ')
#for autoincrement:
# https://stackoverflow.com/questions/32040409/reliable-autoincrementing-identifiers-for-all-nodes-relationships-in-neo4j
# for lock:
#this allows the increment counter (allowing for concurrent transactions to be serialised):
#http://neo4j.com/docs/stable/transactions-isolation.html
#https://stackoverflow.com/questions/35138645/how-to-perform-an-atomic-update-on-relationship-properties-with-py2neo
#https://stackoverflow.com/questions/31798311/write-lock-behavior-in-neo4j-cypher-over-transational-rest-ap
#This was tested by running many threads on the same operation (plot_add)
#without plot_id_lock (as a separate cypher query) it fails to maintain the count properly and clashes with the unique UID constraint
	plot_id_lock = (' MERGE (id:Counter {name:"Plots", uid: "Plots"}) '
		' ON CREATE SET id._LOCK_ = true, id.count=0 '
		' ON MATCH SET id._LOCK_ = true ')
	plot_add = ( 'MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Locations) '
			' -[:SUBMITTED]->(p:Plots),'
		' (f:Farm { name: $farm}) ' 
			' -[:IS_IN]->(:Region { name: $region }) '
			' -[:IS_IN]->(:Country { name: $country }),  '
		' (id:Counter {uid:"Plots"}) '
		' SET id.count = id.count+1 '
		' MERGE (p)-[s:SUBMITTED]-(plot:Plot { name:$plot, uid:id.count}) '
			' -[:IS_IN]->(f)'
			' ON CREATE SET s.time = timestamp() '
		' MERGE (id)-[:COUNTED]->(plot) '
		' SET id._LOCK_ = false' )
##ITEM PROCEDURES
#block procedures
	#no longer need fill path as have unique plotID
	get_blocks = ('MATCH (b:Block) '
			' -[:IS_IN]->(:PlotBlocks) '
			' -[:IS_IN]->(:Plot {uid: toInteger($plotID)})'
		' RETURN properties (b)')
	block_find = ( 'MATCH (block:Block {name : $block}) '
		' -[:IS_IN]->(:PlotBlocks) '
		' -[:IS_IN]->(:Plot {uid : $plotID}) '
	 	' RETURN block' )
	block_id_lock = (' MATCH (p:Plot {uid:$plotID})'
		' MERGE (id:Counter {name:"Blocks", uid: ("plot_" + $plotID + "_blocks")}) '
				'-[:FOR]->(:PlotBlocks)-[:IS_IN]->(p) '
			' ON MATCH SET id._LOCK_ = true '
			' ON CREATE SET id._LOCK_ = true, id.count = 0 ')
	block_add = ('MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Items) '
			' -[:SUBMITTED]->(b:Blocks), '
		' (id:Counter {uid: ("plot_" + $plotID + "_blocks")}) '
			' -[:FOR]->(pb:PlotBlocks) '
		' SET id.count = id.count+1 '
		' MERGE (b)-[s:SUBMITTED]->(:Block {name: $block, '
					' uid:($plotID + "_B" + id.count), '
					' id:id.count}) '
				' -[:IS_IN]->(pb)'
			' ON CREATE SET s.time = timestamp () '
		' SET id._LOCK_ = false')
	get_blocks_csv = ( ' MATCH (b:Block) '
			' -[:IS_IN]->(:PlotBlocks) '
			' -[:IS_IN]->(p:Plot {uid : $plotID}) '
			' -[:IS_IN]->(f:Farm) '
			' -[:IS_IN]->(r:Region) '
			' -[:IS_IN]->(c:Country) '
		' RETURN {UID : b.uid, '
			' PlotID : p.uid, '
			' BlockID : b.id, '
			' Block : b.name, '
			' Plot : p.name, '
			' Farm : f.name, '
			' Region : r.name, '
			' Country : c.name}'
		' ORDER BY b.id')
#tree procedures
	tree_id_lock = (' MATCH (p:Plot {uid: $plotID})'
		' MERGE (id:Counter {name : "Trees", uid: ("plot_" + $plotID + "_trees")}) '
				' -[:FOR]->(:PlotTrees)-[:IS_IN]->(p) '
			' ON CREATE SET id._LOCK_ = true, id.count = 0 '
			' ON MATCH SET id._LOCK_ = true ')
	trees_add = ( 'MATCH (:User {username:$username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:Items) '
				' -[:SUBMITTED]->(ut:Trees), '
			' (pt:PlotTrees)-[:IS_IN]->(p:Plot {uid: $plotID}) '
		#create per user per plot trees node
		' MERGE (ut)-[s1:SUBMITTED]->(upt:UserPlotTrees)<-[:CONTRIBUTED_BY]-(pt) '
			' ON CREATE SET s1.time = timestamp() '
		' WITH p, pt, upt'
		' MATCH (id:Counter {uid: ("plot_" + $plotID + "_trees")}) '
			' -[:FOR]->(pt) '
			' -[:IS_IN]->(p) '
			' -[:IS_IN]->(f:Farm) '
			' -[:IS_IN]->(r:Region) '
			' -[:IS_IN]->(c:Country) '
		# now iterate for the number of trees entered
		' UNWIND range(1, $count) as counter ' 
		' SET id.count=id.count+1 '
		# add UserPlotTrees SUBMITTED trees with IS_IN relationship to PlotTrees
		' MERGE (upt)-[s2:SUBMITTED]->(t:Tree {uid:(toString(p.uid) + "_T" + toString(id.count)), '
					' id:id.count}) '
				' -[:IS_IN]->(pt) '
			' ON CREATE SET s2.time = timestamp() '
		' SET id._LOCK_ = false '
		' RETURN {UID:t.uid, '
			' PlotID:p.uid, '
			' TreeID:t.id, '
			' Plot:p.name, '
			' Farm:f.name, '
			' Region:r.name, '
			' Country:c.name} '
		' ORDER BY t.id')
	#no optional parameters in Neo4j so creating an almost duplicate query where block is given
	block_trees_add = ( 'MATCH (:User {username:$username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:Items) '
				' -[:SUBMITTED]->(ut:Trees), '
			' (pt:PlotTrees)-[:IS_IN]->(p:Plot {uid: $plotID}) '
		#create per user per plot trees node
		' MERGE (ut)-[s1:SUBMITTED]->(upt:UserPlotTrees)<-[:CONTRIBUTED_BY]-(pt) '
			' ON CREATE SET s1.time = timestamp() '
		' WITH p, pt, upt'
		' MATCH (id:Counter {uid: ("plot_" + $plotID + "_trees")}) '
				' -[:FOR]->(pt) '
				' -[:IS_IN]->(p) '
				' -[:IS_IN]->(f:Farm) '
				' -[:IS_IN]->(r:Region) '
				' -[:IS_IN]->(c:Country), '
			' (b:Block {uid : $blockUID})'
		# now iterate for the number of trees entered
		' UNWIND range(1, $count) as counter ' 
		' SET id.count=id.count+1 '
		# add UserPlotTrees SUBMITTED trees with IS_IN relationship to PlotTrees
		' MERGE (upt)-[s2:SUBMITTED]->(t:Tree {uid:(toString(p.uid) + "_T" + toString(id.count)), '
					' id:id.count}) '
				' -[:IS_IN]->(pt) '
			' ON CREATE SET s2.time = timestamp() '
		' MERGE (bt:BlockTrees)-[:IS_IN]-(b)'
		' MERGE (t)-[s3:IS_IN]-(bt)'
			' ON CREATE SET s3.time = timestamp() '
		' SET id._LOCK_ = false '
		' RETURN {UID:t.uid, '
			' PlotID:p.uid, '
			' TreeID:t.id, '
			' Block:b.name, '
			' Plot:p.name, '
			' Farm:f.name, '
			' Region:r.name, '
			' Country:c.name} '
		' ORDER BY t.id')
	trees_get = ( 'MATCH (t:Tree) '
			' -[:IS_IN]->(pt:PlotTrees) '
			' -[:IS_IN]->(p:Plot {uid : $plotID}) '
			' -[:IS_IN]->(f:Farm) '
			' -[:IS_IN]->(r:Region) '
			' -[:IS_IN]->(c:Country) '
		' WHERE t.id >= $start '
		' AND t.id <= $end '
		' OPTIONAL MATCH (t)-[:IS_IN]->(:BlockTrees)-[:IS_IN]->(b:Block)'
##COME BACK HERE WHEN RE_WRITING UPLOAD/DOWNLOAD ETC.
		' OPTIONAL MATCH (t)<-[:DATA_FOR]-(d:Data) '
			'<-[:SUBMISSIONS]-(:PlotTreeTraitData)<-[:PLOT_DATA]-(:TreeTrait {name:"name"})'
		' RETURN { UID : t.uid, '
			' PlotID : p.uid, '
			' TreeID : t.id, '
			' TreeName : d.value, '
			' Block : b.name, '
			' Plot : p.name, '
			' Farm : f.name, '
			' Region : r.name, '
			' Country : c.name } '
		' ORDER BY t.id ' )
#sample procedures
	#these are unique in allowing users to submit other than locations/items/data
	#not sure if good idea, check with Benoit if can get list of tissues and storage procedures instead
	#also if keeping this way good idea to create as a separate container section (not within items, maybe methods or similar?)
	tissue_add = (' MATCH (:User {username:$username})-[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:Items)-[:SUBMITTED]->(sa:Samples) '
		' MERGE (sa)-[s:SUBMITTED]->(:Tissue {name: $tissue}) '
			' ON CREATE SET s.time = timestamp() ' )
	storage_add = ('MATCH (user:User {username:$username})-[:SUBMITTED]->(:Submissions)  '
			' -[:SUBMITTED]->(:Items)-[:SUBMITTED]->(sa:Samples) '
		' MERGE (sa)-[s:SUBMITTED]->(:Storage {name :$storage}) '
			' ON CREATE SET s.time = timestamp() ')
	#regular stuff
	sample_id_lock = ( ' MATCH  (p:Plot {uid: $plotID}) '
		' MERGE (id:Counter {name : "Samples", '
				' uid: ("plot_" + $plotID + "_samples") '
				' }) '
			' -[:FOR]->(:PlotSamples) '
			' -[:FROM_PLOT]->(p) '
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ' )
	samples_add = (' MATCH '
			' (t:Tree) '
				' -[:IS_IN]->(:PlotTrees) '
				' -[:IS_IN]->(:Plot {uid: $plotID}), '
			' (tissue:Tissue {name:$tissue}), '
			' (storage:Storage {name:$storage}), '
			' (:User {username:$username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:Items) '
				' -[:SUBMITTED]->(samples:Samples), '
			' (id:Counter {uid: ("plot_" + $plotID + "_samples")}) '
				' -[:FOR]->(ps:PlotSamples) '
				' -[:FROM_PLOT]->(p:Plot {uid: $plotID}) '
				' -[:IS_IN]->(f:Farm) '
				' -[:IS_IN]->(r:Region) '
				' -[:IS_IN]->(c:Country) '
		# with the selected trees to be sampled
		' WITH t, tissue, storage, samples, id, ps, p, f, r, c '
		' ORDER BY t.id '
		' WHERE t.id >= $start '
		' AND t.id <= $end '
##COME BACK HERE WHEN RE_WRITING UPLOAD/DOWNLOAD ETC.
		' OPTIONAL MATCH (t)<-[:DATA_FOR]-(d:Data) '
			'<-[:SUBMISSIONS]-(:PlotTreeTraitData)<-[:PLOT_DATA]-(:TreeTrait {name:"name"}) '
		#with replicates
		' UNWIND range(1, $replicates) as replicates '
		#incrementing with a plot level counter
		' SET id.count = id.count + 1 '
		#Create samples
		' CREATE (s:Sample { '
				' uid: (p.uid + "_S" + id.count), '
				' id:id.count, '
				' date:$date, '
				' time:$time, '
				' replicates: $replicates '
				'}) '
		#Create a per Tissue per Storage container node
		' MERGE (tissue)<-[:OF_TISSUE]-(TiSt:TissueStorage)-[:STORED_IN]->(storage) '
		#and split this off per PlotSamples
		' MERGE (TiSt)<-[:COLLECTED_AS]-(pts:PlotTissueStorage)-[:FROM]->(ps) '
		#Create a TreeSamples node per tree
		' MERGE (t)<-[:FROM_TREE]-(ts:TreeSamples) '
		#Link the sample to its TreeSamples and PlotTissueSamples containers
		' MERGE (ts)<-[:FROM_TREE]-(s)-[:FROM_PLOT]->(pts) '
		#track user submissions through successive UserPlotSamples then UserTreeSamples container nodes
		' MERGE (samples)-[:SUBMITTED]->(ups:UserPlotSamples)<-[:CONTRIBUTED_BY]-(ps) '
		' MERGE (ups)-[:SUBMITTED]->(uts:UserTreeSamples)<-[:CONTRIBUTED_BY]-(ts) '
		' MERGE (uts)-[s1:SUBMITTED]->(s) '
			' ON CREATE SET s1.time = timestamp() '
		' SET id._LOCK_ = false '
		#return for csv
		' RETURN { '
			' UID : s.uid, '
			' PlotID : p.uid, '
			' TreeID : t.id, '
			' TreeName : d.value, '
			' SampleID : s.id, '
			' Date : s.date, '
			' Tissue : tissue.name, '
			' Storage : storage.name, '
			' Plot : p.name, '
			' Farm : f.name, '
			' Region : r.name, '
			' Country : c.name '
		' } '
		' ORDER BY s.id '
		)
	upload_FB_tree = (
		# load in the csv
		' LOAD CSV WITH HEADERS FROM $filename as csvLine '
		# And identify the plots and traits assessed
		' MATCH (plot:Plot {uid:toInteger(head(split(csvLine.UID, "_")))}), '
				' (tree:Tree {uid:csvLine.UID}),'
				' (trait:TreeTrait {name:csvLine.trait}), '
			' (:User {username : $username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:DataSub) '
				' -[:SUBMITTED]->(fb:FieldBook)'
		' OPTIONAL MATCH (block:Block {name:csvLine.value}) '
		# Create per plot per trait node
		' MERGE (plot)<-[:FROM_PLOT]-(pt:PlotTrait)-[:FOR_TRAIT]->(trait) '
		# Also per tree per PlotTrait node
		' MERGE (tree)<-[:FROM_TREE]-(tt:TreeTreeTrait)-[:FOR_TRAIT]->(pt)'
		#Merge the data point linking to TreeTrait node
		' MERGE (d:Data {tree:csvLine.UID, '
					' value:csvLine.value, '
					' timeFB:csvLine.timestamp, '
					#the below converts the time to epoch (ms) - same as neo4j timestamp() to allow simple math on date/time
					' time:apoc.date.parse(csvLine.timestamp,"ms","yyyy-MM-dd HH:mm:sszzz"), '
					' person:csvLine.person, '
					' location:csvLine.location}) '
				' -[:DATA_FOR]->(tt) '
			#storing whether found or not for user feedback
			' ON CREATE SET d.found = false '
			' ON MATCH SET d.found = true '
		#track user submissions through successive UserPlotTrait then UserTreeTrait containers
		' MERGE (fb)-[:SUBMITTED]->(upt:UserPlotTrait)<-[:CONTRIBUTED_BY]-(pt) '
		' MERGE (upt)-[:SUBMITTED]->(utt:UserTreeTrait)<-[:CONTRIBUTED_BY]-(tt) '
		' MERGE (utt)-[s1:SUBMITTED]->(d) '
			' ON CREATE SET s1.time = timestamp() '
		# if tree data contained block info, link tree to block
		' FOREACH (n IN CASE WHEN csvLine.trait = "block" THEN [1] ELSE [] END | '
				' MERGE (bt:BlockTrees)-[:IS_IN]->(block) '
				' MERGE (tree)-[s2:IS_IN]->(bt) '
					' ON CREATE SET s2.time = timestamp()'
			' ) '
		#And give the user feedback on their submission success
		' RETURN d.found ' 
		)
	upload_FB_block = ( 
		# load in the csv
		' LOAD CSV WITH HEADERS FROM $filename as csvLine '
		# And identify the plots and traits assessed
		' MATCH (:Plot {uid:toInteger(head(split(csvLine.UID, "_")))})-[:CONTAINS_BLOCKS]-(pb:PlotBlocks), '
			' (block:Block {uid:csvLine.UID}),'
			' (trait:BlockTrait {name:csvLine.trait}), '
			' (:User {username : $username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:DataSub) '
				' -[:SUBMITTED]->(fb:FieldBook)'
		# Crete per plot per trait node
		' MERGE (plot)<-[:FROM_PLOT]-(pt:PlotTrait)-[:FOR_TRAIT]-(trait) '
		# Also per block per PlotTrait node
		' MERGE (block)<-[:FROM_BLOCK]-(bt:BlockBlockTrait)-[:FOR_TRAIT]->(pt)'
		# Merge the data point linking to the blocktrait node
		' MERGE (d:Data {block:csvLine.UID, '
					' value:csvLine.value, '
					' timeFB:csvLine.timestamp, '
					' time:apoc.date.parse(csvLine.timestamp,"ms","yyyy-MM-dd HH:mm:sszzz"), '
					' person:csvLine.person, '
					' location:csvLine.location}) '
				' -[:DATA_FOR]->(bt)'
			' ON CREATE SET d.found = false '
			' ON MATCH SET d.found = true '
		# track user submissions through successive UserPlotTrait then UserBlockTrait containers
		' MERGE (fb)-[:SUBMITTED]->(upt:UserPlotTrait)<-[:CONTRIBUTED_BY]-(pt) '
		' MERGE (upt)-[:SUBMITTED]->(utt:UserBlockTrait)<-[:CONTRIBUTED_BY]-(bt) '
		' MERGE (utt)-[s1:SUBMITTED]->(d) '
			' ON CREATE SET s1.time = timestamp() '
		#And give the user feedback on their submission success
		' RETURN d.found' 
		)
	get_submissions_range = ( ' MATCH (:User {username:$username}) '
			' -[:SUBMITTED]->(:Submissions) '
			' -[:SUBMITTED]->(:d)'
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
		'LIMIT 100' )
	get_plots_treecount = (
		' MATCH (C:Country) '
		' OPTIONAL MATCH (C) <-[:IS_IN]-(R:Region) '
		' OPTIONAL MATCH (R) <-[:IS_IN]-(F:Farm) '
		' OPTIONAL MATCH (F) <-[:IS_IN]-(P:Plot) '
		' OPTIONAL MATCH (P) <-[:IS_IN]-(pt:PlotTrees) '
		' OPTIONAL MATCH (pt) <-[:FOR]-(T:Counter {name:"Trees"}) '
		' RETURN '
			' labels(C)[0] as C_label, '
			' labels(R)[0] as R_label, '
			' labels(F)[0] as F_label, '
			' labels(P)[0] as P_label, '
			' C.name, R.name, F.name, P.name, P.uid, T.count ' )