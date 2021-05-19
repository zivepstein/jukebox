library(spotifyr)
library(tidyverse)
#from here

get_track_audio_features_over_100 <- function(ids) {
  
  ## spotifyr limits get_track_audio_features to 100 at a time
  ## this function loops through the full id list
  
  ids <- ids[!is.na(ids)]
  len <- length(ids)
  repetitions <- floor(len/100) * 100
  intervals <- c(seq(from = 0, to = repetitions, by = 100), len)
  
  features <- data.frame()
  for(r in seq_along(intervals)){
    start <- intervals[r]
    end <- intervals[r + 1] - 1
    if(is.na(end)) break
    
    inner_features <- get_track_audio_features(ids = ids[start:end])
    features <- rbind(features, inner_features)
    
  }
  
  return(features)
  
}

id <-'8e8ac879e14447f9b47d9c124b7a9340'
secret <- '780e0c49c0084c3b941d5d56e4c48866'
Sys.setenv(SPOTIFY_CLIENT_ID = id)
Sys.setenv(SPOTIFY_CLIENT_SECRET = secret)
access_token <- get_spotify_access_token()

genres <- c('classical', 'pop', 'electronic', 'folk', 'house', 'rap', 'funk', 'americana')

playlist_ids <- NULL

for(g in genres){
  
  out <- search_spotify(q = g, type = 'playlist', market = 'US', limit = 20)
  out <- out %>% 
    select(name, id) %>%
    mutate(genre =g)
  playlist_ids <- rbind(playlist_ids, out)
}

playlist_songs <- NULL

for(p in seq_along(playlist_ids$id)){
  
  out <- get_playlist_tracks(playlist_id = playlist_ids$id[p])
  
  out <- out %>%
    filter(!is.na(track.id)) %>%
    # separate out the df column artists
    unnest(cols = 'track.artists') %>%
    group_by(track.id) %>%
    mutate(row_number = 1:n(),
           track.artist = name) %>%
    ungroup() %>%
    filter(row_number == 1) %>%
    select(track.id, track.name, track.artist, track.popularity, track.album.id, track.album.name, track.album.release_date) %>%
    mutate(playlist_name = playlist_ids$name[p],
           playlist_id = playlist_ids$id[p],
           playlist_genre = playlist_ids$genre[p]) 
  
  playlist_songs <- rbind(playlist_songs, out)
  
}

playlist_audio <- get_track_audio_features_over_100(ids = playlist_songs$track.id)

playlist_songs <- playlist_songs %>%
  left_join(select(playlist_audio, -track_href, -uri, -analysis_url, -type, -time_signature), by = c('track.id' = 'id')) %>%
  unique() %>%
  filter(!is.na(danceability))

# handle duplicates - songs on multiple playlists
playlist_songs <- playlist_songs %>% 
  group_by(playlist_genre, track.id) %>%
  mutate(row_number = 1:n()) %>%
  filter(row_number == 1) %>%
  ungroup() %>%
  select(-row_number)

#write.csv(playlist_songs, 'genre_songs.csv', row.names=FALSE)
