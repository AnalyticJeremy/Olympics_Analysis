# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/databricks.svg" width="442" /> &nbsp; &nbsp; &nbsp; <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/r.svg" width="90" /><br /><br />
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/olympics.svg" width="550" />
# MAGIC 
# MAGIC # Transform Data
# MAGIC 
# MAGIC Compile the raw data together into a dataset that is more conducive to analytics

# COMMAND ----------

library(SparkR)

# COMMAND ----------

countries <- read.df(path = "/olympics/raw/countries", source = "delta")
disciplines <- read.df(path = "/olympics/raw/disciplines", source = "delta")
edition_discipline_medal_summary <- read.df(path = "/olympics/raw/edition_discipline_medal_summary", source = "delta")
edition_disciplines <- read.df(path = "/olympics/raw/edition_disciplines", source = "delta")
edition_event_medals <- read.df(path = "/olympics/raw/edition_event_medals", source = "delta")
edition_events <- read.df(path = "/olympics/raw/edition_events", source = "delta")
edition_medal_summary <- read.df(path = "/olympics/raw/edition_medal_summary", source = "delta")
editions <- read.df(path = "/olympics/raw/editions", source = "delta")
display(editions)

# COMMAND ----------

# DBTITLE 1,Medal Details
medals <- join(alias(edition_event_medals, "eem"), editions, edition_event_medals$EditionID == editions$EditionID, joinType="left_outer") |>
            filter(!isNull(edition_event_medals$Country)) |>
            select("Year", "Season", "DisciplineID", "Event", "Gender", "Medal", "eem.Country", "Participant")

medals <- join(alias(medals, "m"), disciplines, medals$DisciplineID == disciplines$DisciplineID, joinType="left_outer") |>
            select("Year", "m.Season", "m.DisciplineID", "Discipline", "Event", "Gender", "Medal", "Country", "Participant")

display(medals)

# COMMAND ----------

unlink("/dbfs/olympics/medals", recursive = TRUE);
write.df(medals, path=paste0("dbfs:/olympics/medals"), source="delta", mode="overwrite")

# COMMAND ----------

medal_table <- medals |>
                  group_by("Year", "Season", "DisciplineID", "Gender", "Country", "Medal") |>
                  count() |>
                  group_by("Year", "Season", "DisciplineID", "Gender", "Country") |>
                  pivot("Medal", c("Gold", "Silver", "Bronze")) |>
                  sum("count") |>
                  fillna(0)

medal_table <- join(alias(medal_table, "mt"), disciplines, medal_table$DisciplineID == disciplines$DisciplineID, joinType="left_outer") |>
                  select("Year", "mt.Season", "mt.DisciplineID", "Discipline", "Gender", "Country", "Gold", "Silver", "Bronze")

medal_table <- medal_table |> withColumn("Total", medal_table$Gold + medal_table$Silver + medal_table$Bronze)

# Weighted Score - This is an attempt to provide a score for medal performance by assigning weights to the medals... gold is more valuable than silver
medal_table <- medal_table |> withColumn("Weighted", (medal_table$Gold * 7) + (medal_table$Silver * 4) + (medal_table$Bronze * 2))

medal_table <- medal_table |> arrange(medal_table$Year, medal_table$Season, medal_table$DisciplineID, medal_table$Gender, desc(medal_table$Total), desc(medal_table$Weighted), medal_table$Country)

display(medal_table)

# COMMAND ----------

unlink("/dbfs/olympics/medal_table", recursive = TRUE);
write.df(medal_table, path=paste0("dbfs:/olympics/medal_table"), source="delta", mode="overwrite")

# COMMAND ----------


