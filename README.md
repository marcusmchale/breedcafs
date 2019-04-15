# breedcafs

## Web portal built in Python ([Flask](https://github.com/pallets/flask)) to [Neo4J](https://github.com/neo4j/neo4j) database for BreedCAFS project  
 * Register locations (country,region,farm,plot), blocks, trees and samples.
   * Generates thread safe unique plot/tree/block/sample unique ID's (concatenated to field UID for block/tree/sample).
 * Web forms or spreadsheet files ([XlsxWriter](https://github.com/jmcnamara/XlsxWriter)) to record properties, traits and conditions.
 * [Field-Book](https://github.com/PhenoApps/Field-Book) **field.csv** and **trait.trt** files can be generated to record trait data.
 * Upload collected data as csv or xlsx
   * Input is constrained to specifications of the registered property/trait or condition
   * Feedback on submission success and details for any errors

 * User management
   * Registration/Login/Logout/Password reset (including sessions)
   * Submissions are assigned to user with time-stamped SUBMITTED_BY relationship
   * Group level ("Partner") administration for access control
   * Files generated are emailed to users
   * [WTForms](https://github.com/wtforms/wtforms) validation
 * Dynamic content display and asynchronous tasks
   * Resumable uploads ([Resumable.js](https://github.com/23/resumable.js/))
   * [Celery](https://github.com/celery/celery) asynchronous data integration and user feedback 
   * Context specific form content ([jQuery](https://github.com/jquery/jquery))
   * [D3](https://github.com/d3/d3) powered figures displaying relevant database content
 * Uses the official [Neo4J bolt driver for python](https://github.com/neo4j/neo4j-python-driver)

