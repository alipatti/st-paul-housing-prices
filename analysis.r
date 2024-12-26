# shapefile from:
# https://data-ramseygis.opendata.arcgis.com/datasets/RamseyGIS::attributed-parcels/about

library(tidyverse)
library(magrittr)

load_raw_data <- . %>%
        list.files(full.names = TRUE) %>%
        purrr::map(readxl::read_excel) %>%
        purrr::reduce(bind_rows)

clean_and_filter <- . %>%
        janitor::clean_names() %>%
        filter(property_class == "Residential") %>%
        mutate(
                # integer cols
                across(c(zip_code, asmt_year), as.integer),
                # factors
                across(c(
                        school_district, municipality,
                        land_use, property_class, roll_type
                ), as_factor),
        )

geocode <- . %>%
        mutate(state = "MN") %>%
        tidygeocoder::geocode(
                method = "census",
                street = street,
                postalcode = zip_code,
                city = municipality,
                state = state,
                full_results = TRUE,
        ) %>%
        # clean up API call results
        select(-input_address, -id, -state) %>%
        mutate(across(c(tiger_side, match_type, match_indicator), as_factor))

chunk_size <- 10000
geocode_chunked <- . %>%
        group_by(., chunk = cut_number(sale_date, nrow(.) / chunk_size)) %>%
        group_split() %>%
        purrr:::map(geocode) %>%
        bind_rows() %>%
        select(-chunk)

df <- load_raw_data("data/raw") %>%
        clean_and_filter() %>%
        geocode_chunked()
# arrow::write_parquet("data/geocoded.parquet")
