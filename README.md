# breedcafs

## Portal to database for [BreedCAFS](www.breedcafs.eu) project

## Technology Stack
 * Python web framework ([Flask](https://github.com/pallets))
   * [Celery](https://github.com/celery/celery) asynchronous tasks
   * [WTForms](https://github.com/wtforms/wtforms) validation
 * Graph database back-end with [Neo4J](https://github.com/neo4j/neo4j)
   * also uses some [APOC](https://github.com/neo4j-contrib/neo4j-apoc-procedures) procedures
   * alternatively use [ONgDB](https://github.com/graphfoundation/ongdb)
  
 * Javascript UI
   * [jQuery](https://github.com/jquery/jquery) for dynamic content
   * [jQuery-ui](https://github.com/jquery/jquery-ui) for datepicker, drag and drop sortable lists
   * Resumable uploads ([Resumable.js](https://github.com/23/resumable.js/)) to support large files and poor connections
   * [D3](https://github.com/d3/d3) visualisations of database content

## Core nodes  (data model)
 * Locations:
   - Hierarchical locations (Country > Region > Farm)
   - These provide context for the highest order typical unit of analysis, the Field, which is associated with a Farm.
 * Items:
   - Typical units of analysis (Field > Blocks > Tree > Sample)
   - Pseudo-hierarchical relationships
     - Block provides optional grouping for trees or samples
     - Samples can be representative of Fields, Blocks, Trees or groups of Blocks or Trees (i.e. pools)
     - Samples can further be representative of samples or groups of samples, i.e. sub-divided samples or pooled samples.
 * Records
   * 4 types currently supported:
    * Properties
      - Intransient values
    * Traits
      - Each record corresponds to a single time-point
      - Replicates are supported
    * Conditions
      - Each record corresponds to a period of time
      - Replicates are supported
      - Optionally bound ranges (i.e. optional start and/or end time)
    * Curves
      - Each record corresponds to a single time-point
      - Replicates are supported
      - An independent (X) and dependent (Y) variable, paired.
        - These are stored as a list of X and a list of Y variables that are the same length 
      and in the paired order (ascending X values)
        - Missing values from Y are removed from X
        - Records are not accepted that differ in the value of Y for a given X , for a given replicate at a given X value.
   * Records are stored as nodes connected to the "ItemInput" node on the path between an Item and a Feature
 * Inputs
   * Standardised set of descriptions for all record types
     * Record Type
     * Name
     * Description
     * Format (e.g. numeric, categorical, text)
     * Categories (optional)
   * Currently the details of these are only available internally in the project
 * Input groups
   * Grouping of inputs as related to a task for ease of template generation
   * Partner level management with ability to copy from other partners or a set of default groups

## Services 
 * Web forms and spreadsheets for data collection
   * Spreadsheets ([OpenPyXL](https://openpyxl.readthedocs.io/en/stable/)/[XlsxWriter](https://xlsxwriter.readthedocs.io/)))
   * Upload portal
     * Input is constrained to specifications of the input variable 
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
   * Responsible for approving affiliations to partner (approval can be revoked)
   * Able to add emails allowing new users to register
   * Responsible for managing the partners input groups used for template generation
 
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
  and do not contain characters that Excel does not allow in sheet names: []:*?/\
