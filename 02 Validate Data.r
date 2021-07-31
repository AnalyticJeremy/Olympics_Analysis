# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/databricks.svg" width="442" /> &nbsp; &nbsp; &nbsp; <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/r.svg" width="90" /><br /><br />
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/olympics.svg" width="550" />
# MAGIC 
# MAGIC # Validate Data
# MAGIC 
# MAGIC Now that we have a bunch of data, we want to do a few quick sanity checks to make sure we got everything we were expecting.  We can do this by comparing the summary statistics that we scraped
# MAGIC with the details that we retrieved.  The details should add up to match the summary data.

# COMMAND ----------

library(SparkR)

# COMMAND ----------

countries <- read.df(path = "/olympics/countries", source = "delta")
disciplines <- read.df(path = "/olympics/disciplines", source = "delta")
edition_discipline_medal_summary <- read.df(path = "/olympics/edition_discipline_medal_summary", source = "delta")
edition_disciplines <- read.df(path = "/olympics/edition_disciplines", source = "delta")
edition_event_medals <- read.df(path = "/olympics/edition_event_medals", source = "delta")
edition_events <- read.df(path = "/olympics/edition_events", source = "delta")
edition_medal_summary <- read.df(path = "/olympics/edition_medal_summary", source = "delta")
editions <- read.df(path = "/olympics/editions", source = "delta")
display(editions)

# COMMAND ----------

# DBTITLE 1,Discipline Counts
# Do we have the right number of disciplines at each edition of the games?
ed_sum_df <- edition_disciplines |> group_by(edition_disciplines$EditionID) |> count();
joined_df <- join(alias(ed_sum_df, "esd"), alias(editions, "e"), ed_sum_df$EditionID == editions$EditionID, joinType="outer");
joined_df <- select(joined_df, "esd.EditionID", "count", "e.EditionID", "DisciplineCount");
display(joined_df |> filter(joined_df$count != joined_df$DisciplineCount))

# COMMAND ----------

# DBTITLE 1,Event Counts
# Do we have the right number of events at each edition of the games?
ee_sum_df <- edition_events |> group_by(edition_events$EditionID) |> count();
joined_df <- join(alias(ee_sum_df, "esd"), alias(editions, "e"), ee_sum_df$EditionID == editions$EditionID, joinType="outer");
joined_df <- select(joined_df, "esd.EditionID", "count", "e.EditionID", "MedalEventCount");
display(joined_df |> filter(joined_df$count != joined_df$MedalEventCount))

# COMMAND ----------

# DBTITLE 1,Medals by Country at Edition
sdf <- edition_event_medals |>
          group_by("Country", "EditionID", "Medal") |>
          count() |>
          group_by("Country", "EditionID") |>
          pivot("Medal", c("Gold", "Silver", "Bronze")) |>
          sum("count") |>
          fillna(0)

sdf <- withColumn(sdf, "Total_Calc", sdf$Gold + sdf$Silver + sdf$Bronze);
sdf <- rename(sdf, Gold_Calc = sdf$Gold, Silver_Calc = sdf$Silver, Bronze_Calc = sdf$Bronze);
jdf <- join(sdf, alias(edition_medal_summary, "ems"), sdf$EditionID == edition_medal_summary$EditionID & sdf$Country == edition_medal_summary$NOC, joinType="outer") |> fillna(0)

display(jdf |> filter(jdf$Gold_Calc != jdf$Gold | jdf$Silver_Calc != jdf$Silver | jdf$Bronze_Calc != jdf$Bronze | jdf$Total_Calc != jdf$Total))

# COMMAND ----------


