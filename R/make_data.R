make_jazz_songs_list <- function(){
  s3 <- initialize_s3()
  f <- s3$get_object(Bucket = "scoavoux", 
                     Key = "jazz_songs/jazz_songs.csv")
  jazz_songs <- f$Body %>% rawToChar() %>% data.table::fread() %>% tibble()
  
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

make_songs_2023_data <- function(){
  require(aws.s3)
  require(arrow)
  
  # import short streams
  data_cloud <- arrow::open_dataset(
    source =   arrow::s3_bucket(
      "scoavoux",
      endpoint_override = "minio.lab.sspcloud.fr"
    )$path("records_w3/streams/streams_short"),
    partitioning = arrow::schema(REGION = arrow::utf8())
  )
  query <- data_cloud %>% 
    select(hashed_id, is_listened, media_type, song_id = "media_id") %>% 
    filter(media_type == "song", is_listened == 1) %>% 
    count(hashed_id, song_id, name = "n_play_2023")
  short_stream <- collect(query)  
  return(short_stream)
}
