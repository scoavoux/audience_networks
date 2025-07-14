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
  tar_target(jazz_song_list,      make_jazz_songs_list()),
  tar_target(song_user_2023,      make_jazzsongs_user_count_data(jazz_song_list, .what = "records_2023")),
  tar_target(song_user_geoloc,    make_jazzsongs_user_count_data(jazz_song_list, .what = "geoloc")),
  tar_target(edgelist_2023,       make_edgelist(song_user_2023)),
  tar_target(edgelist_geoloc,     make_edgelist(song_user_geoloc)),
  tar_target(edgelist_2023_csv,   write_data(edgelist_2023, "output/edgelist_2023.csv"), format = "file"),
  tar_target(edgelist_geoloc_csv, write_data(edgelist_geoloc, "output/edgelist_geoloc.csv"), format = "file")
)
  