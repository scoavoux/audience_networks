make_jazz_songs_list <- function(){
  s3 <- initialize_s3()
  f <- s3$get_object(Bucket = "scoavoux", 
                     Key = "jazz_songs/jazz_songs.csv")
  jazz_songs <- f$Body %>% rawToChar() %>% data.table::fread() %>% tibble()
  jazz_songs <- distinct(jazz_songs) %>% 
    filter(!is.na(deezer_id))
  return(jazz_songs)
}

make_survey_data <- function(){
  s3 <- initialize_s3()
  f <- s3$get_object(Bucket = "scoavoux", 
                     Key = "records_w3/survey/RECORDS_Wave3_apr_june_23_responses_corrected.csv")
  survey <- f$Body %>% rawToChar() %>% data.table::fread() %>% tibble()
  
  # filter only 
  survey <- survey %>% 
    filter(Progress == 100,
           country == "FR")
  return(survey)
}

make_jazzsongs_user_count_data <- function(jazz_song_list, .what = c("records_2023", "geoloc")){
  require(aws.s3)
  require(arrow)
  
  .what <- .what[1]
  
  # import short streams
  if(.what == "records_2023"){
    data_cloud <- arrow::open_dataset(
      source =   arrow::s3_bucket(
        "scoavoux",
        endpoint_override = "minio.lab.sspcloud.fr"
      )$path("records_w3/streams/streams_short"),
      partitioning = arrow::schema(REGION = arrow::utf8())
    )
    query <- data_cloud %>% 
      select(hashed_id, is_listened, media_type, song_id = "media_id") %>% 
      filter(media_type == "song", is_listened == 1, song_id %in% jazz_song_list$deezer_id) %>% 
      count(hashed_id, song_id, name = "n_play")
    
  } else if(.what == "geoloc"){
    data_cloud <- arrow::open_dataset(
      source =   arrow::s3_bucket(
        "scoavoux",
        endpoint_override = "minio.lab.sspcloud.fr"
      )$path("records_w3/georecords/stream_hashed"),
      partitioning = arrow::schema(REGION = arrow::utf8())
    )
    query <- data_cloud %>% 
      select(hashed_id, song_id = "media_id") %>% 
      filter(song_id %in% jazz_song_list$deezer_id) %>% 
      count(hashed_id, song_id, name = "n_play")
    
  }
  short_stream <- collect(query)  
  return(short_stream)
}

make_edgelist <- function(song_user){
  song_user <- song_user %>% 
    add_count(hashed_id) %>% 
    filter(n > 1) %>% 
    select(-n)
  d <- song_user %>% 
    rename(song_id1 = song_id, n_play1 = "n_play") %>% 
    left_join(rename(song_user, song_id2 = "song_id", n_play2 = "n_play"))
  r <- d %>% 
    filter(song_id1 != song_id2) %>% 
    count(song_id1, song_id2)
  r <- r %>% 
    mutate(cn = paste0(song_id1, "_", song_id2), 
           cm = paste0(song_id2, "_", song_id1),
           keep = NA)
  for(i in 1:nrow(r)){
    r$keep[i] <- ifelse(r$cm[i] %in% r$cn[1:i], FALSE, TRUE)
  }
  r <- filter(r, keep) %>% 
    select(song_id1, song_id2, n)
  return(r)
}

write_data <- function(data, path){
  write_csv(data, path)
  return(path)
}
