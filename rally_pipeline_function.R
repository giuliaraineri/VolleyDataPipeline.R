rm(list = ls())
#install.packages("datavolley", repos = c("https://openvolley.r-universe.dev","https://cloud.r-project.org"))
library(datavolley)
library(dplyr)
library(tidyr)
library(readxl)

#### MATCHES: regular season 23/24 ####
dir_path <- "C:\\Users\\giuli\\OneDrive - unibs.it\\Desktop\\TESI\\FILE\\TRENTINO SS CORRECTED\\RS"
d <- dir(dir_path, pattern = "dvw$", full.names = TRUE)
#lx = list containing the full contents of every match file (including the match and team metadata)
lx <- lapply(seq_along(d), function(i) {
  message(paste("Reading file", i, "of", length(d)))
  dv_read(d[i], insert_technical_timeouts = FALSE)
})
#px = play-by-play component of each match (but all joined together, which makes it easy to analyze multiple matches at once).
px <- bind_rows(lapply(lx, plays))

# Uploading the other files needed
roster_file <- read_excel("C:/Users/giuli/OneDrive - unibs.it/Desktop/TESI/superlega_roster_23_24.xlsx") 
block_stats <- read.csv("C:/Users/giuli/OneDrive - unibs.it/Desktop/TESI/block_stats.csv")

# Check the structure and view the first rows
str(roster_file)
str(block_stats)
head(roster_file)
head(roster_file)

#-------------------------
# rally_pipeline_function.R
# This file contains all the functions needed to process the px dataset and get rally_data


#-------------- STEP 0: --------------

# Fix the original order of actions,
# setting rally_id = point_id to define the rally number,
# create possession_id to identify to count the number of passes made during the rally.
prepare_px_data <- function(px) {
  px %>%
    # Keep chronological order 
    mutate(original_order = row_number()) %>%  
    
    # Sort by original order
    arrange(original_order) %>%
    
    # Create unique identifier for each rally 
    mutate(rally_id = point_id) %>%
    
    # Group by match and rally
    group_by(match_id, rally_id) %>%
    
    # Track number of possessions (change of team)
    mutate(possession_id = cumsum(team != lag(team, default = first(team)))) %>%  
    ungroup()
}
px <- prepare_px_data(px)


#-------------- STEP 1: --------------
# Create context variables
# Add information about players from dataframe roster_file (uploaded it before, must be a dataframe)
# is_home: determines whether the team is at home or not
# hitters_front_row: determines how many hitters there are in front row
# is_breakpoint, is_sideout, is_high_pressure 
# Modify variable "phase" to get "Serve" when team is serving 
add_context_variables <- function(px, roster_file, high_pressure_threshold = 23) {
  
  if (!is.data.frame(roster_file)) {
    stop("roster_file must be a dataframe")
  }
  px <- px %>%
    dplyr::left_join(
      roster_file %>%
        dplyr::select(player_name, player_number, team, player_role),
      by = c("player_name", "player_number", "team")
    )
  px <- px %>%
    dplyr::mutate(
      is_home = dplyr::if_else(team == home_team, 1L, 0L),
      
      hitters_front_row = dplyr::case_when(
        is_home == 1L & home_setter_position %in% c(1, 5, 6) ~ 3L,
        is_home == 1L & home_setter_position %in% c(2, 3, 4) ~ 2L,
        is_home == 0L & visiting_setter_position %in% c(1, 5, 6) ~ 3L,
        is_home == 0L & visiting_setter_position %in% c(2, 3, 4) ~ 2L,
        TRUE ~ NA_integer_
      ),
      is_breakpoint = dplyr::if_else(point_phase == "Breakpoint", 1L, 0L, missing = 0L),
      is_sideout = dplyr::if_else(point_phase == "Sideout", 1L, 0L, missing = 0L),
      
      is_high_pressure = dplyr::if_else(
        # Case 1: both above the high pressure threshold
        (home_team_score >= high_pressure_threshold & visiting_team_score >= high_pressure_threshold) |
          
          # Case 2: one of the two teams at 24 (set points or close)
          home_team_score == 24 | visiting_team_score == 24 |
          
          # Case 3: 1 point gap in the final set phase
          (
            abs(home_team_score - visiting_team_score) == 1 &
              (
                (set_number < 5 & (home_team_score >= 20 | visiting_team_score >= 20)) |
                  (set_number == 5 & (home_team_score >= 12 | visiting_team_score >= 12))
              )
          ),
        
        1L, 0L
      ),
      
      # Fase di gioco
      phase = dplyr::case_when(
        skill == "Serve" ~ "Serve",
        TRUE ~ "Reception"
      )
    )
  
  return(px)
}
px <- add_context_variables(px, roster_file)  #to modify threshold: px <- add_context_variables(px, roster_file, high_pressure_threshold = 20)


#-------------- STEP 2: --------------
# Create skill variables
add_skill_variables <- function(px) {
  
  # Check if necessary columns are present
  required_columns <- c("skill", "player_name", "evaluation_code", "start_zone", "end_zone")
  missing_columns <- setdiff(required_columns, colnames(px))
  if (length(missing_columns) > 0) {
    stop(paste("Missing required columns in px dataset:", paste(missing_columns, collapse = ", ")))
  }
  
  # Add skill-related variables
  px <- px %>%
    dplyr::mutate(
      # Serve
      serve_player_name     = dplyr::case_when(skill == "Serve" ~ player_name, TRUE ~ NA_character_),
      serve_evaluation_code = dplyr::case_when(skill == "Serve" ~ evaluation_code, TRUE ~ NA_character_),
      serve_start_zone      = dplyr::case_when(skill == "Serve" ~ start_zone, TRUE ~ NA_integer_),
      serve_end_zone        = dplyr::case_when(skill == "Serve" ~ end_zone, TRUE ~ NA_integer_),
      
      # Reception
      reception_player_name      = dplyr::case_when(skill == "Reception" ~ player_name, TRUE ~ NA_character_),
      reception_evaluation_code  = dplyr::case_when(skill == "Reception" ~ evaluation_code, TRUE ~ NA_character_),
      reception_serve_start_zone = dplyr::case_when(skill == "Reception" ~ start_zone, TRUE ~ NA_integer_),
      reception_start_zone       = dplyr::case_when(skill == "Reception" ~ end_zone, TRUE ~ NA_integer_),
      
      # Set
      set_player_name     = dplyr::case_when(skill == "Set" ~ player_name, TRUE ~ NA_character_),
      set_evaluation_code = dplyr::case_when(skill == "Set" ~ evaluation_code, TRUE ~ NA_character_),
      set_start_zone      = dplyr::case_when(skill == "Set" ~ end_zone, TRUE ~ NA_integer_),
      set_end_zone        = dplyr::case_when(skill == "Attack" ~ start_zone, TRUE ~ NA_integer_),
      
      # Attack
      attack_player_name     = dplyr::case_when(skill == "Attack" ~ player_name, TRUE ~ NA_character_),
      attack_evaluation_code = dplyr::case_when(skill == "Attack" ~ evaluation_code, TRUE ~ NA_character_),
      attack_start_zone      = dplyr::case_when(skill == "Attack" ~ start_zone, TRUE ~ NA_integer_),
      attack_end_zone        = dplyr::case_when(skill == "Attack" ~ end_zone, TRUE ~ NA_integer_),
      
      # Block
      block_player_name     = dplyr::case_when(skill == "Block" ~ player_name, TRUE ~ NA_character_),
      block_evaluation_code = dplyr::case_when(skill == "Block" ~ evaluation_code, TRUE ~ NA_character_),
      block_start_zone      = dplyr::case_when(skill == "Block" ~ end_zone, TRUE ~ NA_integer_),
      
      # Dig
      dig_player_name       = dplyr::case_when(skill == "Dig" ~ player_name, TRUE ~ NA_character_),
      dig_evaluation_code   = dplyr::case_when(skill == "Dig" ~ evaluation_code, TRUE ~ NA_character_),
      dig_attack_start_zone = dplyr::case_when(skill == "Dig" ~ start_zone, TRUE ~ NA_integer_),
      dig_end_zone          = dplyr::case_when(skill == "Dig" ~ end_zone, TRUE ~ NA_integer_),
      
      # Freeball
      freeball_player_name     = dplyr::case_when(skill == "Freeball" ~ player_name, TRUE ~ NA_character_),
      freeball_evaluation_code = dplyr::case_when(skill == "Freeball" ~ evaluation_code, TRUE ~ NA_character_),
      freeball_start_zone      = dplyr::case_when(skill == "Freeball" ~ start_zone, TRUE ~ NA_integer_),
      freeball_end_zone        = dplyr::case_when(skill == "Freeball" ~ end_zone, TRUE ~ NA_integer_)
    )
  
  return(px)
}

px <- add_skill_variables(px)


#-------------- STEP 3: --------------
# Aggregate rally_data
# remove row if all skills' variables are NA: it means no action
# add all useful variables
aggregate_rally_data <- function(px) {
  skill_columns <- c(
    "serve_player_name", "serve_evaluation_code", "serve_start_zone", "serve_end_zone",
    "reception_player_name", "reception_evaluation_code", "reception_serve_start_zone", "reception_start_zone",
    "set_player_name", "set_evaluation_code", "set_start_zone", "set_end_zone",
    "attack_player_name", "attack_evaluation_code", "attack_start_zone", "attack_end_zone",
    "block_player_name", "block_evaluation_code", "block_start_zone",
    "dig_player_name", "dig_evaluation_code", "dig_attack_start_zone", "dig_end_zone",
    "freeball_player_name", "freeball_evaluation_code", "freeball_start_zone", "freeball_end_zone"
  )
  rally_data <- px %>%
    group_by(match_id, rally_id, possession_id, team) %>%
    arrange(original_order) %>%
    summarise(
      across(all_of(skill_columns), ~ first(.[!is.na(.)]), .names = "{.col}"),
      
      # Add context variables
      phase                  = first(phase),
      is_breakpoint          = first(is_breakpoint),
      is_sideout             = first(is_sideout),
      is_high_pressure       = first(is_high_pressure),
      hitters_front_row      = first(hitters_front_row),
      home_score_start_of_point     = max(home_score_start_of_point, na.rm = TRUE),
      visiting_score_start_of_point = max(visiting_score_start_of_point, na.rm = TRUE),
      touching_team_is_home  = first(is_home),
      home_team              = first(home_team),
      visiting_team          = first(visiting_team),
      home_setter_position   = first(home_setter_position),
      visiting_setter_position = first(visiting_setter_position),
      serving_team           = first(serving_team),
      is_home                = first(is_home),
      
      # Players in court
      home_p1     = first(home_p1[!is.na(home_p1)]),
      home_p2     = first(home_p2[!is.na(home_p2)]),
      home_p3     = first(home_p3[!is.na(home_p3)]),
      home_p4     = first(home_p4[!is.na(home_p4)]),
      home_p5     = first(home_p5[!is.na(home_p5)]),
      home_p6     = first(home_p6[!is.na(home_p6)]),
      visiting_p1 = first(visiting_p1[!is.na(visiting_p1)]),
      visiting_p2 = first(visiting_p2[!is.na(visiting_p2)]),
      visiting_p3 = first(visiting_p3[!is.na(visiting_p3)]),
      visiting_p4 = first(visiting_p4[!is.na(visiting_p4)]),
      visiting_p5 = first(visiting_p5[!is.na(visiting_p5)]),
      visiting_p6 = first(visiting_p6[!is.na(visiting_p6)])
    ) %>%
    ungroup() %>%
    filter(rowSums(!is.na(pick(all_of(skill_columns)))) > 0) %>%
    group_by(match_id, rally_id) %>%
    mutate(possession_id = row_number() - 1) %>%
    ungroup() %>%
    arrange(match_id, rally_id, possession_id)
  
  return(rally_data)
}
rally_data <- aggregate_rally_data(px)


#-------------- STEP 4: --------------
# Assign player name and role to positions in court
assign_info_to_positions <- function(rally_data, roster_file) {

  rally_data <- rally_data %>%
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p1" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p1_name = player_name, home_p1_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p2" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p2_name = player_name, home_p2_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p3" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p3_name = player_name, home_p3_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p4" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p4_name = player_name, home_p4_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p5" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p5_name = player_name, home_p5_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("home_p6" = "player_number", "home_team" = "team")) %>%
    dplyr::rename(home_p6_name = player_name, home_p6_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p1" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p1_name = player_name, visiting_p1_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p2" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p2_name = player_name, visiting_p2_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p3" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p3_name = player_name, visiting_p3_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p4" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p4_name = player_name, visiting_p4_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p5" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p5_name = player_name, visiting_p5_role = player_role) %>%
    
    left_join(roster_file %>% dplyr::select(player_name, player_number, team, player_role),
              by = c("visiting_p6" = "player_number", "visiting_team" = "team")) %>%
    dplyr::rename(visiting_p6_name = player_name, visiting_p6_role = player_role) %>%
    dplyr::mutate(
      # home_team
      home_p1_role = ifelse(home_setter_position == 4 & home_p1_role == "Spiker", "Opposite", home_p1_role),
      home_p2_role = ifelse(home_setter_position == 5 & home_p2_role == "Spiker", "Opposite", home_p2_role),
      home_p3_role = ifelse(home_setter_position == 6 & home_p3_role == "Spiker", "Opposite", home_p3_role),
      home_p4_role = ifelse(home_setter_position == 1 & home_p4_role == "Spiker", "Opposite", home_p4_role),
      home_p5_role = ifelse(home_setter_position == 2 & home_p5_role == "Spiker", "Opposite", home_p5_role),
      home_p6_role = ifelse(home_setter_position == 3 & home_p6_role == "Spiker", "Opposite", home_p6_role),
      
      # visiting_team
      visiting_p1_role = ifelse(visiting_setter_position == 4 & visiting_p1_role == "Spiker", "Opposite", visiting_p1_role),
      visiting_p2_role = ifelse(visiting_setter_position == 5 & visiting_p2_role == "Spiker", "Opposite", visiting_p2_role),
      visiting_p3_role = ifelse(visiting_setter_position == 6 & visiting_p3_role == "Spiker", "Opposite", visiting_p3_role),
      visiting_p4_role = ifelse(visiting_setter_position == 1 & visiting_p4_role == "Spiker", "Opposite", visiting_p4_role),
      visiting_p5_role = ifelse(visiting_setter_position == 2 & visiting_p5_role == "Spiker", "Opposite", visiting_p5_role),
      visiting_p6_role = ifelse(visiting_setter_position == 3 & visiting_p6_role == "Spiker", "Opposite", visiting_p6_role)
    )
  
  return(rally_data)
}

rally_data <- assign_info_to_positions(rally_data, roster_file)


#-------------- STEP 5: -------------- 
# Assign Block info statistics
assign_opponent_block_efficacy <- function(rally_data, block_stats, our_team) {
  
  # Remove possible duplicates in block_stats
  block_stats_clean <- block_stats %>%
    distinct(player_number, player_name, .keep_all = TRUE) 
  
  # Assign efficacy_index to opponent players in front-row positions (p2, p3, p4)
  rally_data <- rally_data %>%
    mutate(
      opponent_efficacy_index_p2 = ifelse(
        touching_team_is_home == 1,
        block_stats_clean$efficacy_index[match(paste(visiting_p2, visiting_team), paste(block_stats_clean$player_number, block_stats_clean$team))],
        block_stats_clean$efficacy_index[match(paste(home_p2, home_team), paste(block_stats_clean$player_number, block_stats_clean$team))]
      ),
      
      opponent_efficacy_index_p3 = ifelse(
        touching_team_is_home == 1,
        block_stats_clean$efficacy_index[match(paste(visiting_p3, visiting_team), paste(block_stats_clean$player_number, block_stats_clean$team))],
        block_stats_clean$efficacy_index[match(paste(home_p3, home_team), paste(block_stats_clean$player_number, block_stats_clean$team))]
      ),
      
      opponent_efficacy_index_p4 = ifelse(
        touching_team_is_home == 1,
        block_stats_clean$efficacy_index[match(paste(visiting_p4, visiting_team), paste(block_stats_clean$player_number, block_stats_clean$team))],
        block_stats_clean$efficacy_index[match(paste(home_p4, home_team), paste(block_stats_clean$player_number, block_stats_clean$team))]
      )
    ) %>%
    # Replace NA with 0 
    mutate(
      opponent_efficacy_index_p2 = ifelse(is.na(opponent_efficacy_index_p2), 0, opponent_efficacy_index_p2),
      opponent_efficacy_index_p3 = ifelse(is.na(opponent_efficacy_index_p3), 0, opponent_efficacy_index_p3),
      opponent_efficacy_index_p4 = ifelse(is.na(opponent_efficacy_index_p4), 0, opponent_efficacy_index_p4)
    )
  
  return(rally_data)
}

rally_data <- assign_opponent_block_efficacy(rally_data, block_stats, our_team = "Itas Trentino")


#-------------- STEP 7: --------------  
# Assign effective block statistics based on effective position
assign_opponent_effective_block_positions <- function(rally_data) {
  
  # Helper function to assign effective position based on player role and phase
  assign_effective_position <- function(role, position, phase) {
    if (is.na(role)) return(NA_integer_)
    
    if (role == "Middle Blocker") {
      return(3)  # Middle Blocker always in position 3
    } else if (role %in% c("Opposite", "Setter")) {
      # Opposite/Setter logic based on reception phase and position
      if (position == 4 && phase == "Reception") {
        return(4)
      } else {
        return(2)
      }
    } else if (role == "Spiker") {
      # Spiker position logic for Reception and other phases
      if (position == 2 && phase == "Reception") {
        return(2)
      } else {
        return(4)
      }
    } else {
      return(NA_integer_)  # Return NA if no valid role is found
    }
  }
  
  # Update rally_data with effective positions for opponent players
  rally_data <- rally_data %>%
    mutate(
      opponent_p2_role = ifelse(touching_team_is_home == 1, visiting_p2_role, home_p2_role),
      opponent_p3_role = ifelse(touching_team_is_home == 1, visiting_p3_role, home_p3_role),
      opponent_p4_role = ifelse(touching_team_is_home == 1, visiting_p4_role, home_p4_role),
      
      # Assign effective positions based on role and phase
      opponent_effective_p2 = mapply(assign_effective_position, opponent_p2_role, 2, phase),
      opponent_effective_p3 = mapply(assign_effective_position, opponent_p3_role, 3, phase),
      opponent_effective_p4 = mapply(assign_effective_position, opponent_p4_role, 4, phase)
    ) %>%
    rowwise() %>%
    mutate(
      # Initialize efficacy index columns
      opponent_effective_efficacy_index_p2 = NA_real_,
      opponent_effective_efficacy_index_p3 = NA_real_,
      opponent_effective_efficacy_index_p4 = NA_real_,
      
      # Assign efficacy index values to effective positions
      opponent_effective_efficacy_index_p2 = case_when(
        opponent_effective_p2 == 2 ~ opponent_efficacy_index_p2,
        opponent_effective_p3 == 2 ~ opponent_efficacy_index_p3,
        opponent_effective_p4 == 2 ~ opponent_efficacy_index_p4,
        TRUE ~ opponent_effective_efficacy_index_p2
      ),
      
      opponent_effective_efficacy_index_p3 = case_when(
        opponent_effective_p2 == 3 ~ opponent_efficacy_index_p2,
        opponent_effective_p3 == 3 ~ opponent_efficacy_index_p3,
        opponent_effective_p4 == 3 ~ opponent_efficacy_index_p4,
        TRUE ~ opponent_effective_efficacy_index_p3
      ),
      
      opponent_effective_efficacy_index_p4 = case_when(
        opponent_effective_p2 == 4 ~ opponent_efficacy_index_p2,
        opponent_effective_p3 == 4 ~ opponent_efficacy_index_p3,
        opponent_effective_p4 == 4 ~ opponent_efficacy_index_p4,
        TRUE ~ opponent_effective_efficacy_index_p4
      )
    ) %>%
    ungroup()
  
  return(rally_data)
}

rally_data <- assign_opponent_effective_block_positions(rally_data)
