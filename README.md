# Oslo Transit Optimizer

A data science approach to analyzing and optimizing bus routes in Oslo's public transportation system using Python, linear programming, and visualization tools.

## Project Overview

This project aims to:
- Analyze patterns and bottlenecks in selected Oslo bus routes
- Develop optimized route schedules that minimize total travel time
- Visualize results and improvement potential

## Features

- Real-time transit data collection via Entur API
- Historical data analysis using Entur's BigQuery dataset
- Weather impact analysis using Frost API

## Data Sources

- Entur API:
    - Journey planner: https://developer.entur.org/pages-journeyplanner-journeyplanner
    - Historical real-time data: https://data.entur.no/domain/public-transport-data/product/realtime_siri_et/urn:li:container:1d391ef93913233c516cbadfb190dc65
- GTFS data for Ruter: https://developer.entur.org/stops-and-timetable-data
- Weather data from Norwegian Meteorological Institute (Frost API): https://frost.met.no/index.html