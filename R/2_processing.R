#### OTTAWA PROCESSING #########################################################

source("R/0_startup.R")
library(foreach)
doParallel::registerDoParallel()
library(data.table)


# Load previous data ------------------------------------------------------

qload("output/str_raw.qsm", nthreads = availableCores())


# Recalculate active date -------------------------------------------------

daily_active <-
  daily %>%
  filter(status != "B") %>%
  group_by(property_ID) %>%
  filter(date == max(date)) %>%
  slice(1) %>%
  ungroup() %>%
  select(property_ID, active_new = date)

property <-
  property %>%
  left_join(daily_active) %>%
  mutate(active = active_new) %>%
  select(-active_new)

daily <-
  daily %>%
  group_by(property_ID, date, status) %>%
  slice(1) %>%
  ungroup() %>%
  group_by(property_ID, date) %>%
  filter(n() == 1 | (n() == 2 & status != "B")) %>%
  ungroup()

rm(daily_active)


# Add minimum stays -------------------------------------------------------

set.seed(2021)

min_stay_remote <- tbl(.con, "min_stay")

min_stay <- 
  min_stay_remote %>% 
  filter(property_ID %in% !!property$property_ID) %>% 
  collect()

min_stay <- 
  min_stay %>% 
  arrange(property_ID, start_date) %>% 
  group_by(property_ID) %>% 
  mutate(change_date = as.Date(slider::slide_int(
    start_date, ~{
      x <- (.x[1] + 1):.x[2]
      x[sample.int(length(x), 1, 
                   prob = ((.x[1] + 1):.x[2] - as.integer(.x[1])) ^ 0.5)]
    }, 
    .before = 1, .complete = TRUE), origin = "1970-01-01")) %>% 
  ungroup() %>% 
  mutate(start_date = coalesce(change_date, start_date)) %>% 
  select(-change_date)

daily <- 
  daily %>% 
  left_join(min_stay, by = c("property_ID", "date" = "start_date"))

# Get first missing date
min_missing <- 
  daily %>% 
  filter(is.na(minimum_stay)) %>% 
  group_by(property_ID) %>% 
  slice(1) %>% 
  ungroup() %>% 
  select(property_ID, date)

min_fill <- 
  min_stay %>% 
  filter(property_ID %in% min_missing$property_ID) %>% 
  left_join(min_missing, by = "property_ID") %>% 
  group_by(property_ID) %>% 
  filter(start_date < date) %>% 
  filter(start_date == max(start_date)) %>% 
  ungroup() %>% 
  select(property_ID, date, new_min = minimum_stay)

min_extra <- 
  min_missing %>% 
  filter(!property_ID %in% min_fill$property_ID)

min_extra <- 
  min_stay %>% 
  filter(property_ID %in% min_extra$property_ID) %>% 
  group_by(property_ID) %>% 
  slice(1) %>% 
  ungroup() %>% 
  select(property_ID, new_min = minimum_stay) %>% 
  left_join(min_extra, ., by = "property_ID")

min_fill <- 
  bind_rows(min_fill, min_extra) %>% 
  arrange(property_ID)

daily <- 
  daily %>% 
  left_join(min_fill, by = c("property_ID", "date")) %>% 
  mutate(minimum_stay = coalesce(new_min, minimum_stay)) %>% 
  select(-new_min)

daily <- 
  daily %>% 
  arrange(property_ID, date) %>% 
  fill(minimum_stay, .direction = "down")

rm(min_extra, min_fill, min_missing, min_stay, min_stay_remote)


# Calculate multilistings -------------------------------------------------

daily <- 
  daily %>% 
  strr_multi(host) %>% 
  as_tibble()


# Calculate ghost hostels -------------------------------------------------

GH <- strr_ghost(property)


# Add daily status to GH --------------------------------------------------

daily_GH <- 
  daily %>% 
  filter(property_ID %in% unique(unlist(GH$property_IDs)))

setDT(daily_GH)

daily_GH <- daily_GH %>% select(property_ID:status)

status_fun <- function(x, y) {
  status <- unique(daily_GH[date == x & property_ID %in% y, status])
  fcase("R" %in% status, "R", "A" %in% status, "A", "B" %in% status, "B")
}

upgo:::handler_upgo("Analyzing row")

with_progress({
  
  pb <- progressor(nrow(GH))
  
  status <- foreach(i = 1:nrow(GH), .combine = "c") %dopar% {
    pb()
    status_fun(GH$date[[i]], GH$property_IDs[[i]])
  }
  
})

GH$status <- status
GH <- GH %>% select(ghost_ID, date, status, host_ID:data, geometry)

rm(daily_GH, pb, status_fun, status)


# Add GH status to daily --------------------------------------------------

GH_daily <- 
  GH %>% 
  st_drop_geometry() %>% 
  select(date, property_IDs) %>% 
  unnest(property_IDs) %>% 
  mutate(GH = TRUE) %>% 
  select(property_ID = property_IDs, date, GH)

daily <- 
  daily %>% 
  left_join(GH_daily, by = c("property_ID", "date")) %>% 
  mutate(GH = if_else(is.na(GH), FALSE, GH))

rm(GH_daily)


# Split daily into daily and daily_all ------------------------------------

daily_all <- daily
daily <- daily_all %>% filter(minimum_stay < 30, housing)


# Save output -------------------------------------------------------------

qs::qsavem(property, daily, daily_all, GH, file = "output/str_processed.qsm",
           nthreads = availableCores())
