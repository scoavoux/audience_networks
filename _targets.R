# Preparation ------
library(targets)
library(tarchetypes)

tar_option_set(
  packages = c("tidyverse"),
  repository = "aws", 
  repository_meta = "aws",
  resources = tar_resources(
    aws = tar_resources_aws(
      endpoint = Sys.getenv("S3_ENDPOINT"),
      bucket = "scoavoux",
      prefix = "audience_networks"
    )
  )
)

tar_source("R")

# List of targets ------
list(
  tar_target(song_2023, make_songs_2023_data())
)
  