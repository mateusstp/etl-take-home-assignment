[back to the main page](../README.md)

# Storied ETL Exercise
A big part of ETL work is connecting to external APIs, so we can consolidate data into our Snowflake Data Warehouse. We have put together an exercise to give you a chance to demonstrate your ability to look through some API documentation and write a Python script that connects successfully to the App Store Connect API and transfers data into Snowflake.

## Snowflake Setup

In order to accomplish this use case, you will need to create a couple of Snowflake tables.

Create two Snowflake tables and append your initials. For example, if your name were Jose Gonzalez, you would create the following tables:
* `demo.hiring_temp.sales_jg`
* `demo.hiring_prod.sales_jg`

The connection strings you will need to accomplish this task can be found in the Python Environment Variables linked to below. The fields and data types needed for sales tables can be found under **Summary Sales Report** in the App Store Connect Documentation link provided below.

## Use Case

We have a mobile application on the iOS App Store called [Storied: Family History](https://apps.apple.com/us/app/storied-family-history/id1607957410?l=us). We want to capture the sales data each day and store them in Snowflake. We need you to create a Python script that does the following:

1. Connect to the Apple App Store Connect API
2. Download the **Summary Sales Report** for the Storied: Family History App for the month of December 2024
3. Push daily data into the temp table you created for each day in December 2024
4. Merges that data into the production table you created

You should be able to find the information you need to accomplish this task in the App Store Connect documentation linked to below.

A python environment variable script has also been included with the necessary elements to connect a Snowflake instance that has been set up for this exercise, as well as the Apple Connect credentials needed to connect.

Once complete, execute the script and upload your code to GitHub.

## Resources
* **[App Store Connect Documentation](https://developer.apple.com/documentation/appstoreconnectapi)**
* **[Python Snowflake API Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/snowflake-python-overview)**
* **Python Environment Variables to connect to Snowflake and Apple Connect API**

## Note

Typically, you would connect to the App Store Connect API with a p8 file. In this case, we want to use an environment variable instead of a p8 file, so the contents of the p8 file are stored in the APPLE_CONNECT_PRIVATE_KEY variable. You will need to find a way to handle the `\n` in the environment variable so it is interpreted correctly.
