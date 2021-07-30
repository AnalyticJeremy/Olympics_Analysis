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

# COMMAND ----------

# DBTITLE 1,Editions
base_url <- "http://www.olympedia.org";

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

# COMMAND ----------

tables <- list()
urls <- paste0(base_url, editions$Url);

for (i in 1:nrow(editions)) {  
  html <- read_html(urls[i]);
  nodes <- html |> html_node(xpath = "//table[3]");
}


# COMMAND ----------

html |> html_node(xpath = "//table[3]");

# COMMAND ----------

lapply(tables, colnames)

# COMMAND ----------

str(tables)

# COMMAND ----------



content <- read_html("https://en.wikipedia.org/wiki/List_of_highest-grossing_films_in_the_United_States_and_Canada")
tables <- content %>% html_table(fill = TRUE)
first_table <- tables[[1]]
first_table <- first_table[-1,]
library(janitor)
first_table <- first_table %>% clean_names()
first_table %>% 
  mutate(lifetime_gross = parse_number(lifetime_gross)) %>% 
  arrange(desc(lifetime_gross)) %>% 
  head(20) %>% 
  mutate(title = fct_reorder(title, lifetime_gross)) %>% 
  ggplot() + geom_bar(aes(y = title, x = lifetime_gross), stat = "identity", fill = "blue") +
  labs(title = "Top 20 Grossing movies in US and Canada",
       caption = "Data Source: Wikipedia ")
first_table %>% 
  mutate(lifetime_gross_2 = parse_number(lifetime_gross_2)) %>% 
  arrange(desc(lifetime_gross_2)) %>% 
  head(20) %>% 
  mutate(title = fct_reorder(title, lifetime_gross_2)) %>% 
  ggplot() + geom_bar(aes(y = title, x = lifetime_gross_2), stat = "identity", fill = "blue") +
  labs(title = "Top 20 Grossing movies in US and Canada",
       caption = "Data Source: Wikipedia ")
second_table <- tables[[2]]
second_table %>% 
  clean_names() -> second_table
second_table %>% 
  mutate(adjusted_gross = parse_number(adjusted_gross)) %>% 
  group_by(year) %>% 
  summarise(total_adjusted_gross = sum(adjusted_gross)) %>% 
  arrange(desc(total_adjusted_gross)) %>% 
  ggplot() + geom_line(aes(x = year,y = total_adjusted_gross, gr

# COMMAND ----------


