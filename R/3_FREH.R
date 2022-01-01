#### OTTAWA FREH ###############################################################

source("R/0_startup.R")
library(caret)
library(lubridate)
library(slider)


# Load data ---------------------------------------------------------------

qload("output/str_processed.qsm", nthreads = availableCores())


# Prepare daily files -----------------------------------------------------

daily <- 
  daily %>% 
  mutate(year = lubridate::year(date),
         month = lubridate::month(date))


# Get traditional FREH status ---------------------------------------------

FREH <- 
  daily %>% 
  strr_FREH() %>% 
  filter(FREH)


# Produce monthly activity table for all EH listings ----------------------

monthly <-
  daily %>% 
  filter(listing_type == "Entire home/apt") %>% 
  left_join(FREH, by = c("property_ID", "date")) %>% 
  mutate(FREH = if_else(is.na(FREH), FALSE, FREH)) %>% 
  left_join(select(property, property_ID, created),
            by = "property_ID") %>% 
  mutate(created = if_else(lubridate::day(created) == 1, created,
                           floor_date(created, "month") %m+% months(1))) %>% 
  filter(date >= created) %>% 
  mutate(created_year = lubridate::year(created),
         created_month = lubridate::month(created),
         month_since_created = (year - created_year) * 12 + 
           (month - created_month)) %>% 
  group_by(property_ID, year, month) %>% 
  summarize(month_since_created = first(month_since_created),
            R = sum(status == "R"),
            A = sum(status == "A"),
            B = sum(status == "B"),
            FREH = as.logical(ceiling(mean(FREH))),
            .groups = "drop")


# Model based on last 3 months --------------------------------------------

# Summarize by month
after_one_year <- 
  monthly %>% 
  mutate(month = month.name[.data$month], AR = A + R) %>% 
  group_by(property_ID) %>% 
  mutate(R_3 = slide_int(R, sum, .before = 2, .complete = TRUE),
         AR_3 = slide_int(AR, sum, .before = 2, .complete = TRUE)) %>% 
  ungroup() %>% 
  filter(!is.na(R_3), !is.na(AR_3), month_since_created >= 12)


# Fit models and apply to listings > 2 months -----------------------------

model_3 <- glm(FREH ~ R_3 + AR_3 + month, data = after_one_year, 
               family = binomial)

model_3_results <- 
  monthly %>% 
  mutate(month = month.name[.data$month], AR = A + R) %>% 
  group_by(property_ID) %>% 
  mutate(R_3 = slide_int(R, sum, .before = 2, .complete = TRUE),
         AR_3 = slide_int(AR, sum, .before = 2, .complete = TRUE)) %>% 
  ungroup() %>% 
  filter(!is.na(R_3), !is.na(AR_3)) %>% 
  modelr::add_predictions(model_3, type = "response") %>% 
  mutate(FREH_3 = pred) %>% 
  select(-pred) %>% 
  rowwise() %>% 
  mutate(month = which(month.name == month)) %>% 
  ungroup()

daily <-
  daily %>% 
  left_join(select(model_3_results, property_ID, year, month, FREH_3),
            by = c("property_ID", "year", "month")) %>% 
  mutate(FREH_3 = if_else(is.na(FREH_3), 0, FREH_3))

daily <- daily %>% select(-year, -month)


# Model testing -----------------------------------------------------------

# Split the data into training and test set
training_samples_3 <-
  after_one_year$FREH %>% createDataPartition(p = 0.80, list = FALSE)

train_data_3 <- after_one_year[training_samples_3, ]
test_data_3 <- after_one_year[-training_samples_3, ]

# Fit the model
model_3_test <- glm(FREH ~ R_3 + AR_3 + month, data = train_data_3,
                          family = binomial)

# Test models
probabilities_3 <- model_3_test %>% predict(test_data_3, type = "response")
predicted_classes_3 <- ifelse(probabilities_3 > 0.5, "TRUE", "FALSE")
mean(predicted_classes_3 == test_data_3$FREH) |> view()
# Outcome: 0.86


# Save output -------------------------------------------------------------

qs::qsavem(property, daily, daily_all, GH, file = "output/str_processed.qsm",
           nthreads = availableCores())

qs::qsavem(FREH, monthly, after_one_year, model_3, model_3_results,
           file = "output/FREH_model.qsm", nthreads = availableCores())
