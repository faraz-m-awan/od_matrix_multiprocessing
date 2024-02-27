# Calculating OD Matrix (Multiprocessing Enabled)
In order to generate OD and derived products, following two files need to be run in provided order

**1. od_calculation.py**: This file fetches the raw data from the database and performs the following processes:
- Filtering based on impression accuracy and speed between two consecutive GPS points
- Stop node detection
- Save stop node data
- Flow generation between stop nodes (trips)
- Save trip data

**2. trip_extrapolation.py**: After *od_calculation.py* file is done generating trips, *trip_extrapolation.py* file performs the following operations:
- Performance spatial joins for origin and destination of the trips.
- Generating an analysis file containing the information of number of trips made per user
- Save non-aggregated trips
- Save output-area aggregated trips
- Save non-aggregated  stay points
- Save aggregated stay points
- Save trip points
- OD generation
  - Calculate SIMD and Council weights
  - Calculate activity weights
  - Generate OD containing detected trips, simd-council weighted trips, activity weighted trips, and simd-council-activity-weighted trips
 
