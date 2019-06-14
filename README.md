# breedcafs

## Web portal and database for [BreedCAFS](www.breedcafs.eu) project

## Technology Stack
 * Python web framework ([Flask/Jinja2/Werkzeug/ItsDangerous](https://github.com/pallets))
   * [Celery](https://github.com/celery/celery) asynchronous tasks
   * [WTForms](https://github.com/wtforms/wtforms) validation
   
 * Graph database back-end with [Neo4J](https://github.com/neo4j/neo4j)
 * Javascript driven dynamic user interface
   * [jQuery](https://github.com/jquery/jquery) for dynamic content
   * Resumable uploads ([Resumable.js](https://github.com/23/resumable.js/))
   * [D3](https://github.com/d3/d3) visualisations

## Core nodes  (data model)
 * Locations:
   - Heirarchical locations (Country > Region > Farm)
   - These provide context for the highest order typical unit of analysis, the Field, which is associated with a Farm.
 * Items:
   - Typical units of analysis (Field > Blocks > Tree > Sample)
   - Pseudo-heirarchical relationships 
     - Block provides optional grouping for trees
     - Samples can be representative of Fields, Trees or groups of Trees (i.e. pools)
     - Samples can further be representative of samples, i.e. sub-divided samples.
 * Records
   * 3 types currently supported:
    * Properties
      - Intransient values
    * Traits
      - Each record corresponds to a single time-point
      - Replicates are supported
    * Conditions
      - Each record corresponds to a period of time
      - Un-bound ranges (i.e. no start and/or end time) are supported
    * Curves
      - An independent (X) and dependent (Y) variable, paired.
      - These are stored as a list of X and a list of Y variables that are the same length 
      and in the paired order (ascending X values)
      - Missing values from Y are removed from X
      - Records are not accepted that differ in the value of Y at a given time, for a given replicate at a given X value.
   * Records are stored as nodes connected to the "ItemFeature" node on the path between an Item and a Feature
 * Features
   * Standardised set of descriptions for properties, traits and conditions
     * Record Type
     * Name
     * Description
     * Format (e.g. numeric, categorical, text)
     * Categories (optional)
   * Currently these details are only available internally in the project

## Services 
 * Web forms, spreadsheets and [Field-Book](https://github.com/PhenoApps/Field-Book) app for data collection
   * Spreadsheets ([OpenPyXL](https://openpyxl.readthedocs.io/en/stable/)/[XlsxWriter](https://xlsxwriter.readthedocs.io/)) 
 support all record types (properties, traits and conditions)
   * [Field-Book](https://github.com/PhenoApps/Field-Book) **field.csv** and **trait.trt** files can be generated to record trait data.
   * Upload collected data as csv or xlsx
     * Input is constrained to specifications of the feature 
     * Conflicts with existing records are prevented 
     * Feedback on submission success with details for any errors
 * User management
   * Registration/Login/Logout/Password reset (including sessions)
   * Group level ("Partner") administration

## Access control, monitoring and curation
### Core concepts
 * Users
   * Access the service is restricted to users with registered email addresses.
   * All record submissions are stored with a direct, timestamped relationship to the responsible user
 * Partners
   * All users have a Primary Affiliation to a registered project Partner
   * Users may request additional affiliations to other Partners 
 * Partner Admins 
   * Users may be elevated to "Partner Admins"
   * Responsible for approving affiliations (approval can be revoked) 
   * Able to add emails allowing new users to register
 
### Implementation
 * Access control
   * Any records submitted by a User with a Primary Affiliation to a given Partner are accessible
    to all users with a confirmed affiliation to that Partner.
 * Curation
   * Records are marked as deleted rather than being removed from the graph,
     - This allows either restoring the deleted data or later pruning of the graph as desired
   * Any properties that modify relationships in the graph (e.g. "Assign to block")
    are reverted when records are "deleted"
   * Users may "delete" their own records
   * Partner Admins may "delete" any record submitted by users with a primary affiliation to their corresponding Partner 
   
### Notes
  * Ensure feature names for "curve" record type features are not more than 31 characters 
  and do not contain characters that excel does not allow in sheet names: []:*?/\
