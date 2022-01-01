#### OTTAWA IMPORT #############################################################

source("R/0_startup.R")
library(cancensus)
library(osmdata)


# Import geometry ---------------------------------------------------------

# ON province
province <-
  get_census("CA16", regions = list(PR = "35"), geo_format = "sf") |> 
  st_transform(32617) |> 
  select(geometry)

# Ottawa CSD
city <-
  get_census(
    dataset = "CA16", regions = list(CSD = "3506008"), geo_format = "sf") |> 
  st_transform(32617) |> 
  select(GeoUID, Dwellings) |> 
  set_names(c("GeoUID", "dwellings", "geometry")) |> 
  st_set_agr("constant")

# Ottawa DA
DA <-
  get_census(
    dataset = "CA16", regions = list(CSD = "3506008"), level = "DA",
    geo_format = "sf") |> 
  st_transform(32617) |> 
  select(GeoUID, Dwellings) |> 
  set_names(c("GeoUID", "dwellings", "geometry")) |> 
  st_set_agr("constant")


# Toronto neighbourhoods --------------------------------------------------

nbhd <- read_sf(paste0("data/nbhd/Ottawa_Neighbourhood_Study_(ONS)_-_",
                       "Neighbourhood_Boundaries_Gen_2.shp")) |> 
  st_transform(32617) |> 
  select(nbhd_ID = ONS_ID, nbhd = Name, population = POPEST, geometry) |> 
  st_make_valid()
  
nbhd <- 
  DA |> 
  select(dwellings) |> 
  st_interpolate_aw(nbhd, extensive = TRUE) |> 
  st_drop_geometry() |> 
  select(dwellings) |> 
  cbind(nbhd) |> 
  as_tibble() |> 
  st_as_sf() |> 
  relocate(dwellings, .after = population)

DA <- 
  DA |> 
  st_intersection(st_geometry(st_union(active_nbhd)))
  
# Streets
streets <-
 (getbb("Ottawa") * c(1.01, 0.99, 0.99, 1.01)) %>%
 opq(timeout = 200) %>%
 add_osm_feature(key = "highway") %>%
 osmdata_sf()

streets <-
 rbind(
   streets$osm_polygons %>% st_set_agr("constant") %>% st_cast("LINESTRING"),
   streets$osm_lines) %>%
 as_tibble() %>%
 st_as_sf() %>%
 st_transform(32617) %>%
 st_set_agr("constant") %>%
 st_intersection(city)

streets <-
 streets %>%
 filter(highway %in% c("primary", "secondary")) %>%
 select(osm_id, name, highway, geometry)


# Import STR data ---------------------------------------------------------

upgo_connect()

property <- 
  property_remote |> 
  filter(country == "Canada", region == "Ontario", city == "Ottawa") |> 
  collect() |> 
  strr_as_sf(32617) |> 
  st_filter(city) |> 
  mutate(created = if_else(is.na(created), first_active, created),
         scraped = if_else(is.na(scraped), last_active, scraped)) |> 
  filter(!is.na(created))

daily <- 
  daily_remote |> 
  filter(property_ID %in% !!property$property_ID) |> 
  collect() |> 
  strr_expand()

host <-
  host_remote |> 
  filter(host_ID %in% !!property$host_ID) |> 
  collect() |> 
  strr_expand()

upgo_disconnect()

exchange_rates <-
  convert_currency(start_date = min(daily$date),
                   end_date = max(daily$date))

daily <-
  daily |> 
  mutate(year_month = substr(date, 1, 7)) |> 
  left_join(exchange_rates) |> 
  mutate(price = price * exchange_rate) |> 
  select(-year_month, -exchange_rate)

# Run raffle to assign a DA to each listing
set.seed(219092)
property <-
  property |> 
  strr_raffle(DA, GeoUID, dwellings, seed = 1)

# Add nbhd to property file
property <-
  property |> 
  st_join(select(nbhd, nbhd))

# Add nbhd to daily file
daily <-
  property |> 
  st_drop_geometry() |> 
  select(property_ID, nbhd) |> 
  right_join(daily, by = "property_ID")

# Need to manually update housing status
property <- 
  property |> 
  select(-housing) |> 
  strr_housing() |> 
  relocate(housing, .after = last_active)

daily <- 
  daily |> 
  select(-housing) |> 
  left_join(select(st_drop_geometry(property), property_ID, housing)) |> 
  relocate(housing, .after = listing_type)


# Save output -------------------------------------------------------------

qs::qsavem(province, DA, city, nbhd, streets, file = "output/geometry.qsm", 
           nthreads = availableCores())

qs::qsave(exchange_rates, file = "output/exchange_rates.qs")

qs::qsavem(property, daily, host, file = "output/str_raw.qsm", 
       nthreads = availableCores())
