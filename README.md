A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that updates a database (Airtable) and a CRM (Pipedrive) with data obtained from Google Analytics (website traffic for SEO & marketing purposes). The steps it follows are explained - concisely - below (see code for full detail):

• imports a given Airtable table into a variable, using the Airtable API

• creates a list of values containing the email address belonging to the Google Analytics ID (visitor of the website - henceforth GAID - posted to Airtable via front end tools), and the GAID

• connects to Google Analytics (henceforth GA) via the Google API, and looks for the corresponding data pertaining to the GAID - appending it to the same list

• updates the corresponding row (containing the relevant email address and GAID previously fetched) in Airtable with the values obtained from GA (specifically: Source, Medium, Channel Group, Campaign, Keyword -- all relating to SEO & marketing)

• connects to Pipedrive via the Pipedrive API, and iterates through the emails contained in the previous list - searching for the matching Person ID (a unique identifier for a contact in Pipedrive)

• if a Person in Pipedrive is found - it appends the Person ID, together with all the other data corresponding to that Person contained in the previous list, as well as the 2nd part of that Person's GAID (a GAID consists of a string of numbers separated by a dot), to a new list

• as a new GAID is created even though the email address might be the same - e.g. if that Person accesses the website with a different device - we sort the list by the 2nd part of a given Person's GAID in ascending order, and remove all but the first occurrence thereof

• we then query the Pipedrive API using the Person ID and update each respective field in the Person entity with the data we have just obtained

• posts relevant updates/actions to a dedicated Slack (messaging service) channel, as a message, via the Slack API.

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.
