# Pedestrian and Cyclist Safety

## Motivation

In 2018, Bicycling magazine rated Seattle the countries best city for cyclists.  Denver, the city I live in, dropped three positions to 14.  Cyclist safety is an [issue](https://denverite.com/2019/07/31/traffic-deaths-are-having-a-moment-in-denver-its-the-latest-in-a-scroll-of-preventable-deaths/) here in Denver.  I count my self lucky that I am able to ride my bike to work.  Yet, with cyclist fatalities in the news, I wondered how do Denver and Seattle compare in terms of cyclist and pedestrian fatalities?  And, how have those trends changed over time?   This project aims to investigate these !questions.



## Goals

1. Analyze the number of fatal traffic accidents per capita, how they have varied over time, and compare the rates in Denver, Colorado and Seattle, Washington.  

1. Investigate the number of pedestrian and bicycle accidents that occur after dark in Denver and Seattle.  

1. Plot the distribution of pedestrian and bicycle accidents as a function of speed limit.  



## Per capita pedestrain and cyclist fatality rates



## Does lighting conditions a factor?



## Data Sources

To meet these goals, I am relying on the follow data sets.

* [Fatality Analysis Reporting System (FARS)](https://www.nhtsa.gov/research-data/fatality-analysis-reporting-system-fars) - A nationwide census of fatal motor vehicle accicents compiled by the National Highway Traffic Safety Administration (NHTSA) with data provided by the states.  You can find the documention [here](https://crashstats.nhtsa.dot.gov/#/DocumentTypeList/23) and three reports in particular were especially important for my analysis:

    - [Fatality Analysis Reporting System (FARS)  Analytical User’s Manual, 1975-2018 (NHTSA)](https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812827)

    - [Fatality Analysis Reporting System (FARS) Auxiliary Datasets Analytical User’s Manual 1982-2018 (NHTSA)](https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812829)

    - [2018 FARS/CRSS Pedestrian Bicyclist Crash, Typing Manual, A Guide for Coders Using the FARS/CRSS Ped/Bike Typing Tool Revision Date: June 2019 (NHTSA)](https://crashstats.nhtsa.dot.gov/Api/Public/ViewPublication/812809)

* [U.S. Population Data - 1969-2017](https://seer.cancer.gov/popdata/) - Historical population data for the U.S. at the county level.

* [GLCs for the U.S. and U.S. Territories](https://www.gsa.gov/reference/geographic-locator-codes/glcs-for-the-us-and-us-territories) - Source of Geographic Location Codes provided by the U.S. General Services Administration.  

## Citations

* [The Best Bike Cities in America (2018)](https://www.bicycling.com/culture/a23676188/best-bike-cities-2018/) - Rates and compares American cities in terms of their Bike safety, friendliness, energy and culture. Seattle is #1 having risen from #6 in 2017, while Denver has dropped three places to #14.

* [Traffic deaths are having a moment in Denver. It’s the latest in a series of preventable deaths](https://denverite.com/2019/07/31/traffic-deaths-are-having-a-moment-in-denver-its-the-latest-in-a-scroll-of-preventable-deaths/)

* [Cycling lanes reduce fatalities for all road users, study shows](https://www.sciencedaily.com/releases/2019/05/190529113036.htm) - A comprehensive 13 year study of 12 cities looked at factors affecting cyclist safety and finds that protected bikes lanes are the most effective at reducing fatalities.

* [Road Mileage - VMT - Lane Miles, 1900 - 2017 (FHWA)](https://www.fhwa.dot.gov/policyinformation/statistics/2017/pdf/vmt421c.pdf) - Vehicle miles traveled is growing rapidly in the US.  The growth in milage of roads is modest by comparison.

* [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/) - Adopting the data organization scheme from this standardized approach to datascience projects.

* [CSVJSON - Online tool to convert your CSV or TSV formatted data to JSON](Online tool to convert your CSV or TSV formatted data to JSON.) - Converted FPRR_GLC data set from CSV to JSON.
