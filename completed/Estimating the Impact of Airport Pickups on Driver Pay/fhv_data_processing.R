library(arrow)
library(dplyr)
library(lubridate)
library(assertthat)

# import the dataset
data <- read_parquet("./data/raw/fhvhv_tripdata_2022-06.parquet")

# import the zone mapping
zones <- read.csv("./data/raw/taxi+_zone_lookup.csv")

# join pickup zone mapping to dataframe
data <- data %>% left_join(zones, by = c("PULocationID" = "LocationID"))

# rename pickup columns
data <- data %>% rename(
  pickup_Borough = Borough,
  pickup_Zone = Zone,
  pickup_service_zone = service_zone)

# join dropoff zone mapping to dataframe
data <- data %>% left_join(zones, by = c("DOLocationID" = "LocationID"))

# rename dropoff columns
data <- data %>% rename(
  dropoff_Borough = Borough,
  dropoff_Zone = Zone,
  dropoff_service_zone = service_zone)

# map the licenses to for hire drive companies
data <- data %>% 
  mutate(company = case_when(
    hvfhs_license_num == "HV0003" ~ "Uber",
    hvfhs_license_num == "HV0005" ~ "Lyft",
    TRUE ~ "Other"))

# confirm that data only has Uber and Lyfts
assert_that(data %>% filter(company == "Other") %>% nrow() == 0)

# create column that adds tips with driver pay
data <- data %>% mutate(total_pay = driver_pay + tips)

# add wait time, the time between when a ride was requested and started.
data <- data %>% mutate(wait_time = pickup_datetime - request_datetime)

# want to filter out the following instances:
# 1. Zero and negative driver pay
# 2. Rides where the airport fee is not either 0 or 2.5
# 3. WAV rides
# 4. Short times between pickups and dropoff (60 secs or less)
# 5. Rides with a distance of 0
# 6. Rides with negative wait time
# 7. Rides with erroneous pay and trip duration combos
data <- data %>% filter(driver_pay > 0)
  # 59,515 observations dropped

data <- data %>% filter (airport_fee == 0 | airport_fee == 2.50)
  # 14,973 observations dropped

data <- data %>% filter(wav_match_flag == 'N')
  # 1,010,474 observations dropped

data <- data %>% filter(trip_time > 60)
  # 1,629 observations dropped

data <- data %>% filter(trip_miles >0)
  # unknown observations dropped

data <- data %>% filter(wait_time >0)
  # unknown observations dropped

data <- data %>% filter(!(trip_time > 120 & (driver_pay) <=3))
  # unknown dropped

# create a flag to identify airport pickups
# this is true under two criteria
# 1. Pickup location is either JFK (pickup id 132) or LaGuardia (pickup id 138)
# 2. Airport fees are non-zero and it is not a dropoff at any of the airport locations (1, 132, 138) as
# we want to isolate only airport pickups and not dropoffs since these are controllable by the driver
data <- data %>% mutate(
  airport_pickup = PULocationID %in% c(132,138) |
    (airport_fee != 0 & !(DOLocationID %in% c(1,132,138))))

# create hour column based on pickup time
data <- data %>% mutate(pickup_hour = hour(pickup_datetime))

# create time of day flags consistent with daily peaks and low periods
data <- data %>% mutate(time_of_day = case_when(
  between(pickup_hour,0,4) ~ "early_morning",
  between(pickup_hour,5,11) ~ "morning",
  between(pickup_hour,12,14) ~ "midday",
  between(pickup_hour,15,17) ~ "afternoon",
  between(pickup_hour,18,20) ~ "evening",
  TRUE ~ "late_night"))

# set seed so we all get the same result
set.seed(12345)

# take a sample of 2M rows without replacement (half training/half confirmation)
total_sample <- sample_n(tbl = data, size = 2000000, replace = FALSE)

# drop unused variables for space
total_sample = total_sample %>% select(-c(hvfhs_license_num, dispatching_base_num, originating_base_num, request_datetime, 
                                          on_scene_datetime, pickup_datetime, dropoff_datetime, PULocationID, DOLocationID,
                                          wav_request_flag, wav_match_flag,base_passenger_fare, tolls, bcf, sales_tax, 
                                          pickup_service_zone, dropoff_service_zone, pickup_Borough, pickup_Zone, 
                                          dropoff_Borough, dropoff_Zone))

# first half training
training_sample <- head(x = total_sample, n = 1000000)

# second half confirmation
confirm_sample <- tail(x = total_sample, n = 1000000)

# write sample to csv
write.csv(x = training_sample, file = "./data/processed/cleaned_sample_data.csv")

# write confirmation to csv
write.csv(x = confirm_sample, file = "./data/processed/cleaned_confirm_data.csv")