library(spotifyr)
library(tidyverse)
#from here
songs <- read.csv("~/github/jukebox/data-science/songs.csv")

get_artist_audio_features2 <- function (artist = NULL, include_groups = "album", return_closest_artist = TRUE, 
          dedupe_albums = TRUE, authorization = get_spotify_access_token()) 
{
  if (is_uri(artist)) {
    artist_info <- get_artist(artist, authorization = authorization)
    artist_id <- artist_info$id
    artist_name <- artist_info$name
  }
  else {
    artist_ids <- search_spotify(artist, "artist", authorization = authorization)
    if (return_closest_artist) {
      artist_id <- artist_ids$id[1]
      artist_name <- artist_ids$name[1]
    }
    else {
      choices <- map_chr(1:length(artist_ids$name), function(x) {
        str_glue("[{x}] {artist_ids$name[x]}")
      }) %>% paste0(collapse = "\n\t")
      cat(str_glue("We found the following artists on Spotify matching \"{artist}\":\n\n\t{choices}\n\nPlease type the number corresponding to the artist you're interested in."), 
          sep = "")
      selection <- as.numeric(readline())
      artist_id <- artist_ids$id[selection]
      artist_name <- artist_ids$name[selection]
    }
  }
  artist_albums <- get_artist_albums(artist_id, include_meta_info = TRUE, authorization = authorization)
  print(artist_albums)
  num_loops_artist_albums <- ceiling(artist_albums$total/20)
  if (num_loops_artist_albums > 1) {
    artist_albums <- map_df(1:num_loops_artist_albums, function(this_loop) {
      get_artist_albums(artist_id, include_groups = include_groups, 
                        offset = (this_loop - 1) * 20, authorization = authorization)
    })
  }
  else {
    artist_albums <- artist_albums$items
  }
  if (dedupe_albums) {
    artist_albums <- dedupe_album_names(artist_albums)
  }
  album_tracks <- map_df(artist_albums$album_id, function(this_album_id) {
    album_tracks <- get_album_tracks(this_album_id, include_meta_info = TRUE, 
                                     authorization = authorization)
    num_loops_album_tracks <- ceiling(album_tracks$total/20)
    if (num_loops_album_tracks > 1) {
      album_tracks <- map_df(1:num_loops_album_tracks, 
                             function(this_loop) {
                               get_album_tracks(this_album_id, offset = (this_loop - 
                                                                           1) * 20, authorization = authorization)
                             })
    }
    else {
      album_tracks <- album_tracks$items
    }
    album_tracks <- album_tracks %>% mutate(album_id = this_album_id, 
                                            album_name = artist_albums$album_name[artist_albums$album_id == 
                                                                                    this_album_id]) %>% rename(track_name = name, 
                                                                                                               track_uri = uri, track_preview_url = preview_url, 
                                                                                                               track_href = href, track_id = id)
  })
  dupe_columns <- c("duration_ms", "type", "uri", "track_href")
  num_loops_tracks <- ceiling(nrow(album_tracks)/100)
  track_audio_features <- map_df(1:num_loops_tracks, function(this_loop) {
    track_ids <- album_tracks %>% slice(((this_loop * 100) - 
                                           99):(this_loop * 100)) %>% pull(track_id)
    get_track_audio_features(track_ids, authorization = authorization)
  }) %>% select(-dupe_columns) %>% rename(track_id = id) %>% 
    left_join(album_tracks, by = "track_id")
  artist_albums %>% mutate(artist_name = artist_name, artist_id = artist_id) %>% 
    select(artist_name, artist_id, album_id, album_type, 
           album_images = images, album_release_date = release_date, 
           album_release_year, album_release_date_precision = release_date_precision) %>% 
    left_join(track_audio_features, by = "album_id") %>% 
    mutate(key_name = pitch_class_lookup[key + 1], mode_name = case_when(mode == 
                                                                           1 ~ "major", mode == 0 ~ "minor", TRUE ~ as.character(NA)), 
           key_mode = paste(key_name, mode_name))
}

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

genres <- unique(songs$genre)

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
genre_songs <- playlist_songs %>% 
  group_by(playlist_genre, track.id) %>%
  mutate(row_number = 1:n()) %>%
  filter(row_number == 1) %>%
  ungroup() %>%
  select(-row_number)

write.csv(genre_songs, "~/github/jukebox/data-science/data/genre_songs.csv")



artists <- unique(c(songs$dna_artist,songs$artist))
artists_ids <- NULL
for(a in artists){
  out <- search_spotify(q = a, type = 'artist', market = 'US', limit = 20)
  
  
  
  out <- out %>% 
    select(name, id) %>%
    mutate(artist =a, artist_id = id)
  artists_ids <- rbind(artists_ids, out)
}

artists_ids <- artists_ids[c(1,2,3,25,43,48,59,79,90),] #entity collision
write.csv(artists_ids, "~/github/jukebox/data-science/data/artists_ids.csv") 

artist_songs <- NULL
for(p in seq_along(artists_ids$id)){
  
  out <- get_artist_top_tracks(artists_ids$id[p])
  out$artist_id <- artists_ids$id[p]
  
  artist_songs <- rbind(artist_songs, out)
}
features <- get_track_audio_features_over_100(ids = artist_songs$id)
artist_songs_out <- features %>% left_join(artist_songs, by ='id') %>% left_join(artists_ids, by ='artist_id') %>% select(-artists,-album.artists,-album.images)
write.csv(artist_songs_out, "~/github/jukebox/data-science/data/artist_songs.csv") 

get_artist_top_tracks('3vTmy4w7PxWEuFDFg2HcVG')
a = search_spotify(q = "nobide", type = 'artist', market = 'US', limit = 20)
