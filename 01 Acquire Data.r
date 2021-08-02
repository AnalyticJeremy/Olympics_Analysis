# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/databricks.svg" width="442" /> &nbsp; &nbsp; &nbsp; <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/r.svg" width="90" /><br /><br />
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/olympics.svg" width="550" />
# MAGIC 
# MAGIC # Acquire Data
# MAGIC 
# MAGIC We can't analyze data that we don't have... so let's get some data on historic Olympic performance.  We will use [Olympedia](https://www.olympedia.org/) as our source.
# MAGIC This data was compiled by many Olympic history enthusiasts.  I am grateful for their work.  All I have done is scrape the data into a format more conducive for analysis.
# MAGIC 
# MAGIC Since the data doesn't change very often (only once every two years), we will download the data and save a local copy.  We will save it to DBFS as Delta tables so we can use it later.

# COMMAND ----------

library(tidyverse)
library(rvest)

base_url <- "https://www.olympedia.org";
datasets <- list();

# COMMAND ----------

# DBTITLE 1,Disciplines
html <- read_html(paste0(base_url, "/sports"));
nodes <- html |> html_nodes(xpath = "//table");

table <- (html_table(nodes, fill = TRUE))[[1]];
colnames(table) <- c("DisciplineID", "Discipline", "Sport", "Season", "OlympicStatus");

sport_urls <- nodes |> html_nodes("td:nth-child(3)") |> html_node("a") |> html_attr("href");
table$SportId <- sub("^/sport_groups/([A-Z0-9]+)$", "\\1", sport_urls);

table$OlympicStatus <- str_extract(nodes |> html_nodes("td:nth-child(5)") |> html_node("span") |> html_attr("class"), "[^-]+$") == "ok";

datasets$disciplines <- table;

# COMMAND ----------

# DBTITLE 1,Countries
html <- read_html(paste0(base_url, "/countries"));
nodes <- html |> html_nodes(xpath = "//table[1]");

table <- (html_table(nodes, fill = TRUE))[[1]];
colnames(table) <- c("CountryID", "Country", "CompetedInModernOlympics");

table$CompetedInModernOlympics <- str_extract(nodes |> html_nodes("td:nth-child(3)") |> html_node("span") |> html_attr("class"), "[^-]+$") == "ok";

# This will be controversial... but I want to a way to analyze the performance of nations over time.  Due to geopolitical changes, nations sometimes change codes.
# I am attempting to link these together with a common ID.  But this is highly subjective.
# For example, I will say that the old USSR "became" Russia.  I'm sure others will disagree.
table$CountryGroupID <- table$CountryID;
table[table$CountryID=="URS", "CountryGroupID"] <- "RUS";
table[table$CountryID=="EUN", "CountryGroupID"] <- "RUS";
table[table$CountryID=="ROC", "CountryGroupID"] <- "RUS";
table[table$CountryID=="SAA", "CountryGroupID"] <- "GER";
table[table$CountryID=="GDR", "CountryGroupID"] <- "GER";
table[table$CountryID=="FRG", "CountryGroupID"] <- "GER";
table[table$CountryID=="BOH", "CountryGroupID"] <- "CZE";
table[table$CountryID=="TCH", "CountryGroupID"] <- "CZE";
table[table$CountryID=="ANZ", "CountryGroupID"] <- "AUS";

datasets$countries <- table;

# COMMAND ----------

# DBTITLE 1,Editions
html <- read_html(paste0(base_url, "/editions"));

tables <- list();

for (i in 1:2) {
  nodes <- html |> html_nodes(xpath = paste0("//table[", i, "]"));
  table <- (nodes |> html_table(fill = TRUE))[[1]];
  
  colnames(table) <- sub('^$', "Note", colnames(table));
  
  table$Url <- nodes |> html_nodes("td:nth-child(1)") |> html_node("a") |> html_attr("href");
  table$Country <- nodes |> html_nodes("td:nth-child(4) img") |> html_attr("src");
  table$Country <- sub("^/images/flags/([A-Z]+)\\.png$", "\\1", table$Country)
  
  tables[[i]] <- table;
}

tables[[1]]$Season <- "Summer";
tables[[2]]$Season <- "Winter";

editions <- rbind(tables[[1]], tables[[2]]);
editions <- editions |> filter(Competition != "—");

editions$EditionID <- as.numeric(sub("^/editions/([0-9]+)$", "\\1", editions$Url));

# COMMAND ----------

# DBTITLE 1,Edition Details
edition_disciplines   <- data.frame();
edition_medal_summary <- data.frame();
editions$ParticipantNote <- NA;
editions$MedalEventsNote <- NA;

urls <- paste0(base_url, editions$Url);

for (i in 1:nrow(editions)) {
  edition_id <- editions$EditionID[i];
  
  html <- read_html(urls[i]);
  
  fact_table <- html |> html_node(xpath = "//h2[text()='Facts']/following-sibling::table") |> html_table();
  editions[editions$EditionID == edition_id, "ParticipantNote"] <- gsub("\n", " ", fact_table[fact_table$X1 == "Participants", 2]);
  editions[editions$EditionID == edition_id, "MedalEventsNote"] <- gsub("\n", " ", fact_table[fact_table$X1 == "Medal events", 2]);
  
  disp_urls <- html |> html_node(xpath = "//h2[text()='Medal Disciplines']/following-sibling::table") |> html_nodes("td a") |> html_attr("href");
  disp_ids <- str_extract(disp_urls, "([^\\/]+$)");
  edition_disciplines <- rbind(edition_disciplines, data.frame(EditionID = edition_id, DisciplineID = disp_ids));
  
  medal_table <- html |> html_node(xpath = "//h2[text()='Medal table']/following-sibling::table") |> html_table();
  medal_table$EditionID <- edition_id;
  edition_medal_summary <- rbind(edition_medal_summary, medal_table[,2:ncol(medal_table)]);
}

editions$ParticipantCount <- as.numeric(str_extract(editions$ParticipantNote, "^[0-9]+"));
editions$CountryCount     <- as.numeric(str_extract(editions$ParticipantNote, " [0-9]+ "));
editions$MedalEventCount  <- as.numeric(str_extract(editions$MedalEventsNote, "^[0-9]+"));
editions$DisciplineCount  <- as.numeric(str_extract(editions$MedalEventsNote, " [0-9]+ "));

datasets$editions <- editions;
datasets$edition_disciplines <- edition_disciplines;
datasets$edition_medal_summary <- edition_medal_summary;

# COMMAND ----------

# DBTITLE 1,Edition Events
datasets$edition_events <- data.frame();
datasets$edition_discipline_medal_summary <- data.frame();
datasets$edition_event_medals <- data.frame();

for (i in 1:nrow(edition_disciplines)) {
  edition_id    <- edition_disciplines[i, "EditionID"];
  discipline_id <- edition_disciplines[i, "DisciplineID"];
  
  url <- paste0(base_url, "/editions/", edition_id, "/sports/", discipline_id);
  html <- read_html(url);
  
  edition_event_nodes <- html |> html_node(xpath = "//h2[text()='Events']/following-sibling::table");
  edition_event_table <- edition_event_nodes |> html_table(fill = TRUE);
  edition_event_table$ResultID <- as.numeric(str_extract(edition_event_nodes |> html_nodes("td:nth-child(1)") |> html_node("a") |> html_attr("href"), "[^/]+$"));
  edition_event_table$EditionID <- edition_id;
  edition_event_table$DisciplineID <- discipline_id;
  edition_event_table <- edition_event_table[-nrow(edition_event_table), ];
  datasets$edition_events <- rbind(datasets$edition_events, edition_event_table);
  
  edition_discipline_medal_summary_nodes <- html |> html_node(xpath = "//h2[text()='Medal table']/following-sibling::table");
  edition_discipline_medal_summary_table <- edition_discipline_medal_summary_nodes |> html_table(fill = TRUE);
  edition_discipline_medal_summary_table$EditionID <- edition_id;
  edition_discipline_medal_summary_table$DisciplineID <- discipline_id;
  col_names <- names(edition_discipline_medal_summary_table);
  col_names[2] <- "Country";
  names(edition_discipline_medal_summary_table) <- col_names;
  datasets$edition_discipline_medal_summary <- rbind(datasets$edition_discipline_medal_summary, edition_discipline_medal_summary_table);

  # Parse the medal winners in each event
  colors <- c("Gold", "Silver", "Bronze");

  event_medals <- data.frame();
  medal_nodes <- html |> html_node(xpath = "//h2[text()='Medals']/following-sibling::table");
  medal_rows <- medal_nodes |> html_nodes("tr");

  for (j in 2:length(medal_rows)) {
    row <- medal_rows[j];
    
    event_link <- row |> html_node("td:nth-child(1) a");
    event_name <- html_text(event_link);
    result_id <- as.numeric(str_extract(event_link |> html_attr("href"), "[^/]+$"));
    
    for (k in seq_along(colors)) {
      col_index <- k * 2;
      athlete_text <- row |> html_node(paste0("td:nth-child(", col_index, ")"))     |> html_nodes(xpath=".//text()") |> html_text();
      country_text <- row |> html_node(paste0("td:nth-child(", col_index + 1, ")")) |> html_nodes(xpath=".//text()") |> html_text();
      
      athlete_text <- ifelse(athlete_text == "—", NA, athlete_text);
      country_text <- ifelse(athlete_text == "—", NA, country_text);
      
      for (m in seq_along(athlete_text)) {
        datasets$edition_event_medals <- rbind(datasets$edition_event_medals, data.frame(
          EditionID = edition_id,
          DisciplineID = discipline_id,
          ResultID = result_id,
          Event = event_name,
          Medal = colors[k],
          Participant = athlete_text[m],
          Country = country_text[m]
        ));
      }
    }
  }
}

datasets$edition_event_medals$Gender <- trimws(str_extract(datasets$edition_event_medals$Event, "[^,]*$"))
datasets$edition_events$Gender <- trimws(str_extract(datasets$edition_events$Event, "[^,]*$"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to Delta
# MAGIC Now that we've scraped all of the data into R dataframes, we want to write the data to disk for future analysis.

# COMMAND ----------

library(SparkR)

unlink("/dbfs/olympics", recursive = TRUE);
dir.create("/dbfs/olympics");

for (df_name in names(datasets)) {
  df <- createDataFrame(datasets[[df_name]]);
  write.df(df, path=paste0("dbfs:/olympics/raw/", df_name), source="delta", mode="overwrite")
}

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/olympics/raw"))

# COMMAND ----------


