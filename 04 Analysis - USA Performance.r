# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/databricks.svg" width="442" /> &nbsp; &nbsp; &nbsp; <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/r.svg" width="90" /><br /><br />
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/olympics.svg" width="550" /><br /><br />
# MAGIC <img src="https://raw.githubusercontent.com/AnalyticJeremy/Olympics_Analysis/main/img/logos/us_flag.svg" width="550" />
# MAGIC 
# MAGIC # Analysis &mdash; USA Performance
# MAGIC 
# MAGIC At the risk of being an "ugly American," I want to use the acquired data to explore how the US has performed at the Olympics.

# COMMAND ----------

library(SparkR)

# COMMAND ----------

medals <- read.df(path=paste0("dbfs:/olympics/medals"), source="delta", mode="overwrite")
medal_table <- read.df(path=paste0("dbfs:/olympics/medal_table"), source="delta", mode="overwrite")
display(medals)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Has the US Finished in the Medal Standings Over the Years?

# COMMAND ----------

df <- medal_table |>
        groupBy("Year", "Season", "Country") |>
        sum("Gold", "Total", "Weighted")

df <- df |> rename(Gold = df$`sum(Gold)`, Total = df$`sum(Total)`, Weighted = df$`sum(Weighted)`)
df <- df |> arrange(df$Year, df$Season, desc(df$Weighted), df$Country)



ws <- orderBy(windowPartitionBy("Year", "Season"), desc(df$Gold))
df <- df |> withColumn("Gold_Rank", over(rank(), ws))

ws <- orderBy(windowPartitionBy("Year", "Season"), desc(df$Total))
df <- df |> withColumn("Total_Rank", over(rank(), ws))

ws <- orderBy(windowPartitionBy("Year", "Season"), desc(df$Weighted))
df <- df |> withColumn("Weighted_Rank", over(rank(), ws))

first <- df |> filter(df$Weighted_Rank == 1) |> groupBy("Year", "Season") |> agg(Winner = min(df$Country))
usa_df <- join(alias(df, "df"), first, df$Year == first$Year & df$Season == first$Season, joinType="left_outer") |>
            selectExpr("df.*",  "Winner") |>
            filter(df$Country == "USA") |>
            orderBy("Year", "Season")

display(usa_df)

# COMMAND ----------

# DBTITLE 1,In Which Editions Was US the Most Dominant?
# We will define "dominant" as the percentage of available medals won

totals_df <- df |> groupBy("Year", "Season") |> agg(Total_Gold = sum(df$Gold), Total_Total = sum(df$Total), Total_Weighted = sum(df$Weighted))

x <- join(alias(df, "df"), alias(totals_df, "t"), df$Year == totals_df$Year & df$Season == totals_df$Season, joinType="outer") |>
        selectExpr("df.Year", "df.Season", "df.Country", "df.Gold / t.Total_Gold AS Gold_Perc", "df.Gold_Rank", "df.Total / t.Total_Total AS Total_Perc", "df.Total_Rank", "df.Weighted / t.Total_Weighted AS Weighted_Perc", "df.Weighted_Rank") |>
        filter("Country = 'USA'")

x <- x |> arrange(desc(x$Weighted_Perc))

display(x)

# COMMAND ----------

# DBTITLE 1,In Which Sports Has US Won the Most Medals?
x <- medal_table |>
        filter("Country = 'USA'") |>
        groupBy("Season", "DisciplineID", "Discipline") |>
        agg(Gold = sum(medal_table$Gold), Total = sum(medal_table$Total), Weighted = sum(medal_table$Weighted))
x <- x |> arrange(desc(x$Total))
display(x)

# COMMAND ----------

# DBTITLE 1,In Which Sports is US the Most Dominant?
totals_df <- medal_table |>
                groupBy("Season", "DisciplineID", "Discipline") |>
                agg(Total_Gold = sum(medal_table$Gold), Total_Total = sum(medal_table$Total), Total_Weighted = sum(medal_table$Weighted))

x <- medal_table |>
        filter("Country = 'USA'") |>
        groupBy("Season", "DisciplineID", "Discipline") |>
        agg(Gold = sum(medal_table$Gold), Total = sum(medal_table$Total), Weighted = sum(medal_table$Weighted))

x <- join(alias(x, "x"), alias(totals_df, "t"), x$Season == totals_df$Season & x$DisciplineID == totals_df$DisciplineID, joinType="outer") |>
       fillna(0) |>
       selectExpr("t.Season", "t.DisciplineID", "t.Discipline", "Gold / Total_Gold AS Gold_Perc", "Total / Total_Total AS Total_Perc", "Weighted / Total_Weighted AS Weighted_Perc")

x <- x |> arrange(desc(x$Total_Perc))

display(x)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Questions
# MAGIC  - Performance of men vs women
# MAGIC  - Which sports do women excel in vs men?
# MAGIC  - Longest medal streaks
# MAGIC  - Longest medal draughts
# MAGIC  - Which events has US won largest percentage of medals?
# MAGIC  - What sports did US once dominate but no longer does (or vice versa)?
# MAGIC  - Which sports is US worst in?
