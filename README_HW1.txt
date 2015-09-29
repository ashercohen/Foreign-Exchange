11676 Big Data Alalytics - Assignment #1 - Data Preparation
===========================================================

Asher Cohen asherc@cs.cmu.edu

General:
--------
My solution is a series of Map-Reduce jobs (cascade) generating the data matrix, written in Java using the Cascading framework (www.cascading.org).
The solution can generate the data matrix based on any subset of the currency pair.
Label: EUR/USD directionality
Features: min, max, close values for both ask and bid for non EUR/USD currency pairs.
Time interval: 1 minute.

Files:
------
Main.java - Main class and entry point of the data preparation flow is defined and executed.
Currencies.java - enum representing the currencies pairs
EuroUSDAverageFunction.java - function (mapper) to calculate avg of min,max,close values of EUR/USD. This value will help determine directionality
EuroUSDFilter.java - Filter to retain only rows that are EUR/USD related
EuroUSDLabelBuffer.java - Buffer (reducer) that calculates the directionality of EUR/USD between consecutive time intervals
FeaturesGeneratorBuffer.java - Buffer (reducer) that generates one row of features from multiple rows that all belong to the same time interval but have different currencies pairs
InputParser.java - Function (mapper) that parses input line into tuples.
MinMaxCloseBuffer.java - Buffer (reducer) that calculates the min,max and close values, both for ask and bid, at a single time interval for a fixed currencies pair
OtherCurrenciesFilter.java - Filter to retain only rows that are not EUR/USD

Sample of output matrix:
-----------------------
features are based on currencies pairs: AUD/JPY, AUD/USD, AUD/NZD
labels are directionality of EUR/USD pair

* The solution can generate features that are based on any currencies pairs - this is for demonstration purposes only.

83.44,83.47,83.47,83.54,83.57,83.57,,,,,,,0.89766,0.89801,0.89801,0.89804,0.89832,0.89832,0
83.47,83.47,83.47,83.57,83.58,83.58,,,,,,,0.89806,0.89808,0.89806,0.89832,0.89836,0.89836,1
,,,,,,,,,,,,0.89803,0.89803,0.89803,0.89834,0.89834,0.89834,0
83.4,83.44,83.44,83.54,83.56,83.54,,,,,,,0.89697,0.89765,0.89765,0.89803,0.89852,0.89803,0
83.44,83.45,83.44,83.55,83.55,83.55,,,,,,,,,,,,,0


