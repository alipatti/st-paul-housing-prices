library(tidyverse)
library(tidygeocoder)

load_raw_data <- . %>%
        list.files(full.names = T) %>%
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

get_lat_long <- . %>%
        mutate(state = "MN") %>%
        geocode(
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

df <- load_raw_data("data") %>%
        clean_and_filter()

geocoded <- df %>%
        head(100) %>%
        get_lat_long()
