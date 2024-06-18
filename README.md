# Data Engineer - Case Study

## Context

You are a Data Engineer at Xeneta. Xeneta collects data about ocean and air shipping contracts.

Your goal is to load some initial data from the given files to a database
(we recommend DuckDB for quick local development)
and create daily regional aggregated prices based on port-to-port contract data with the help of DBT.

Please provide your solution as a git repository, and take notes about how long time you spend on the case study.


## Data Details

We are providing you with a small set of simplified, randomized, almost-real-world data.
A some database dumps are provided in the `input_files` folder as CSV files that
include the following information:

### Ports

Information about cargo ports, including:

* PID: an integer ID for the port, unique
* 5-character port code, also unique
* Port name
* Slug identifying which region the port belongs to
* Country
* Two-letter country code

### Regions

A hierarchy of regions that Xeneta has defined, based on ocean container shipping routes and pricing patterns.
You can think about this hierarchy as a few top-level regions (those have Null as their "parent region"), and a tree
of smaller regions under them.
Regions have the following information:

* Slug - a machine-readable form of the region name
* The name of the region
* Slug describing which parent region the region belongs to in Xeneta's region hierarchy

Note that a region can have both ports and regions as children, and the region tree does not have a fixed depth.

### Exchange rates

* Day on which the exchange rate is valid
* Three-letter currency code
* Exchange Rate for converting a price in a given currency to USD

### Datapoints

This is a Xeneta concept, a "datapoint" describes metadata for an ocean shipping contract between a company who wants to ship cargo in containers, and a service supplier who owns and manages the container boxes and shipping vessels.

* Created: timestamp when this shipping contract info landed in our system
* D_ID: a unique ID for a datapoint (identifies a shipping contract)
* Origin and Destination Port-IDs (PID-s): identify where the containers to be moved start and end their journey
* Contract validity: `valid_from` and `valid_to` dates between which the cargo will be shipped for this contract's price, usually a month or a year, but can be shorter or longer as well
* Company ID: is an integer ID for the company that wants to move cargo as of this contract (the "buyer" of this shipping contract)
* Supplier ID: integer ID for the vessel operator company that provides the shipping service (the "seller" of this contract)
* Equipment ID: an integer between 1 and 6 inclusive, identifies the type of the container (also known as equipment) that is used for transportation. Our users always want to get aggregated price levels for one specific container type of their interest.

### Charges

Each shipping contract (a.k.a. datapoint) has a set of associated _charges_ that we have
to sum up to get a total price for the contract's value. The charges might be in different currencies - for example a "fee for loading the cargo at the origin port" will likely have a different currency than "fee for offloading the containers at the destination port" as the ports are typically in different countries.

* D_ID is the identifier of the datapoint/contract that this charge belongs to
* Currency is the three-letter currency code to use
* Value is the numerical value of this part of the full price

## Your tasks as a data engineer

*End goal:* make our data consumers happy by providing easy to consume, up-to-date data to them.
What the users are interested in: average and median daily prices of ocean shipping contracts, for a given equipment type, between any two port(s) or region(s).
For brevity, we use the concept of a _transportation lane_ or just _lane_ : it means a pair of origin and destination locations, so from any port/region to any other port/region. Note that pricing is usually different if we transport goods from A to B versus B to A, so we have to make this distinction.

*Specification details*:
 - Identify which datapoints are valid for each day when we have any data (in the sample dataset provided, valid days will be between 2021-01-01 and 2022-06-01).
 - Calculate the price level of each datapoint in _USD_ each day by taking all of the charges that belong to the datapoint, converting them to _USD_ based on the day's exchange rate, and summing up the USD value of charges for each individual day. This means that the price value of a shipping contract might fluctuate a bit in our system within its validity date range, depending on daily exchange rate adjustments.
 - Example conversion from EUR to USD: `USD_value` =  `EUR value` / `"rate"` in exchange rates data provided

*General requirements:*
 - Load all files to the database of your choice (we recommend DuckDB)
 - Make sure that you have exactly these three schemas in the database:
    * `raw`
    * `staging`
    * `final`
 - Contents from the CSV files should be loaded to `raw` schema without any changes.
 - Do initial data validations and checks in `staging` schema.
 - Final outputs such as `prices` and `aggregations` models should be placed in `final` schema (see descriptions and requirements for these in the following subsections).
 - In our scenario we imagine that regions, ports, and exchange rates are all known in advance and these
parts of the data are not changing. On the other hand, new contracts (datapoints and their corresponding charges) are added to the system regularly. We assume no datapoints or charges are deleted.

### Load and transform data

Initially, you should load these data files to the DB:
- ports
- regions
- exchange_rates
- datapoints_1 (only this one from datapoints!)
- charges_1 (only this one from charges!)

Based on the raw data from the files above, provide _average_ and _median_ daily prices for each equipment type, on any lane where there is any data available, calculated in _USD_. Save this data in a table in the `final` schema.

### Help your data users

Provide a few example queries to the data users. For example, how can they get the average container shipping price of equipment type _2_ from China East Main region to US West Coast region?

### Data update: new contracts

- Load charges_2 and datapoints_2 data to the DB and your models should pick up on these changes, make calculations accordingly. Check your assumptions!


### Requirement update

Our users are not entirely happy, because now a data update might cause a sudden big jump in the aggregated price levels they see, and this causes confusion sometimes.

Data Scientists have looked into the issue and have found: If we make sure to only provide aggregated data to
end users if we have _sufficient coverage_ then the new datapoint additions will have a smaller effect and
in general will make our data more reliable.

In particular: _we should only show aggregated price values if we have at least 5 different companies and 2 different suppliers providing data on the given lane, with given equipment type_ .

Implementing this update will make us lose the ability to show any aggregated prices on some lanes and parameter combinations, but it's okay.

For internal use cases, we are still interested in partially covered lanes though.
So your task is:
 - Add a new _boolean_ column, named `dq_ok` (meaning "data quality: OK") to your table in the `final` schema that holds the aggregated prices. This column should have True if and only if the new data quality requirement is true for that given row (at least 5 different companies and 2 different suppliers provide data for that day and lane and equipment type).

When implementing this: Be prepared for a future update! Maybe next quarter, with more data incoming, we could increase the requirements for sufficient coverage, to have even more robust data.

### Optional tasks

In case you have some extra time, not required :)

### Request for metadata

Our Data Scientists want to keep an eye on how data coverage is developing with new data imports of contracts.
Provide a way to show them the number of covered lanes (number of `(origin, destination, equipment type, day)` combinations with `dq_ok = True`) both _before_ and _after_ a data update with new data (meaning when a new batch of data is arriving, receiving a new pair of files with datapoints and their charges).
Hint: use :hook: -s!

### What if?

Think about the more realistic scenario of _not_ knowing all exchange rates beforehand.
How would you make sure to calculate prices in the future part of a contract? How much more complicated would the data transformation become with daily exchange rate updates?
