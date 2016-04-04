#Setting up the path for sparkR in RStudio
Sys.setenv(SPARK_HOME="/Users/rutvikparmar/spark")
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#including the spark package
library(SparkR)
library(d3Network)

#Setting up the spark
sc <- sparkR.init(master = "local")

#initializing spark Context
sqlContext <- sparkRSQL.init(sc)

# Wikipedia Clickstream dataset. 
# The data shows how people get to a Wikipedia article and what articles they click on next. 
# In other words, it gives a weighted network of articles, where each edge weight corresponds 
# to how often people navigate from one page to another.

# Format

# The data includes the following 6 fields:
  
# prev_id: if the referer does not correspond to an article in the main namespace of English Wikipedia, this value will be empty. Otherwise, it contains the unique MediaWiki page ID of the article corresponding to the referer i.e. the previous article the client was on
# curr_id: the unique MediaWiki page ID of the article the client requested
# n: the number of occurrences of the (referer, resource) pair
# prev_title: the result of mapping the referer URL to the fixed set of values described above
# curr_title: the title of the article the client requested
# type
"link- if the referer and request are both articles and the referer links to the request
 redlink - if the referer is an article and links to the request, but the request is not in the produiction enwiki.page table
 other - if the referer and request are both articles but the referer does not link to the request. This can happen when clients search or spoof their refer"

# Reading the file and storing it in the dataframe
ClickstreamDF <- read.df(sqlContext, "file:///Users/rutvikparmar/Downloads/2015_02_clickstream.tsv","com.databricks.spark.csv", inferSchema = "true", delimiter = "\\t", header = "true")

#Displaying top 20 rows
showDF(ClickstreamDF)

#counting the number of rows in the file
count(cache(ClickstreamDF))

#Caching the data in the memory for fast retrieval
cache(ClickstreamDF)

#again counting the rows to show how spark leverages it core to speed up its processing
count(ClickstreamDF)

#Print Schema
printSchema(ClickstreamDF)

#Selecting the required columns for further analysis and displaying it again 
ClickstreamDF2 <- select(ClickstreamDF, "prev_title", "curr_title", "n", "type")

ClickstreamDF2$n <- cast(ClickstreamDF2$n, "Int")
ClickstreamDF2$n <- alias(ClickstreamDF2$n, "n")

showDF(ClickstreamDF2)
count(cache(ClickstreamDF2))


#Registering it in a temporary table so as to use with sqlContext
registerTempTable(ClickstreamDF2, "clickstream")

#************************************************************************************************************************************************

#Question 1 - What are the top 10 articles requested from wikipedia ?

        top_10_articles <- sql(sqlContext, "SELECT curr_title as Title, SUM(n) as Top_Articles FROM clickstream GROUP BY curr_title ORDER BY Top_Articles DESC LIMIT(5)")
        showDF(top_10_articles)
        
        top_articles <- collect(top_10_articles)
        
        top_articles_astable <- as.table(top_articles$Top_Articles)
        
        barplot(top_articles_astable, col = c("lightblue", "mistyrose","lightcyan", "lavender"), names.arg = c(top_articles$Title), cex.names = 0.6, xlab = "Articles", ylab = "Number of times articles clicked", main = "Top 5 Articles")
      
        
#************************************************************************************************************************************************        

#Question 2 - Who sent the most traffic to wikipedia in feb2015 ? So, Who were the top referers to wikipedia ?

        top_referers <- sql(sqlContext, "SELECT prev_title as Referers, SUM(n) as Number_of_times_refered FROM clickstream GROUP BY prev_title ORDER BY Number_of_times_refered DESC LIMIT(10)")
        showDF(top_referers)
        
        top_10_referers <- collect(top_referers)
        
        refererdf <- data.frame(top_10_referers)

        count <- as.table(refererdf$Number_of_times_refered)
        
        barplot(count,scales=list(x = list(log = 10)), col = c("lightblue", "mistyrose","lightcyan", "lavender"), names.arg = c(refererdf$Referers), cex.names = 0.6, xlab = "Referers", ylab = "Number of times refered", main = "Top 10 Referers")
       
#************************************************************************************************************************************************        
        
#Question 3 - What were the top 5 trending articles on twitter Feb 2015 ?

        top_twitter_articles <- sql(sqlContext, "SELECT curr_title as Top_articles_in_twitter, SUM(n) as Number_of_times_refered FROM clickstream WHERE prev_title = 'other-twitter' GROUP BY curr_title ORDER BY Number_of_times_refered DESC LIMIT(5)")
        showDF(top_twitter_articles)

#************************************************************************************************************************************************        
        
#Question 4 - What are the most requested missing pages ?

        showDF(sql(sqlContext, "SELECT DISTINCT type FROM clickstream"))
        top_missing_pages <- sql(sqlContext, "SELECT curr_title as Top_missing_pages, SUM(n) as Number_of_times_refered FROM clickstream WHERE type = 'redlink' GROUP BY curr_title ORDER BY Number_of_times_refered DESC LIMIT(10)")
        showDF(top_missing_pages)

#************************************************************************************************************************************************        
        
#Question 5 - What does the traffic inflow vs outflow look like for the most requested pages ?

        #Lets first find out pageviews per article
        page_views_df <- sql(sqlContext, "SELECT curr_title as Article, SUM(n) as in_count FROM clickstream GROUP BY curr_title")
        registerTempTable(page_views_df,"intable")
        cache(page_views_df)
        showDF(page_views_df)
        
        #then, find the link clicks per article
        link_click_per_article_df <- sql(sqlContext, "SELECT prev_title as link_clicked, SUM(n) as out_count FROM clickstream GROUP BY prev_title")
        registerTempTable(link_click_per_article_df,"outtable")
        cache(link_click_per_article_df)
        showDF(link_click_per_article_df)
        #so, when the clients went to the 'David_Janson' article 340 times they clicked on a link in that article to go to next article
        
        #Join the 2 dataframes to get the wholistic picture
        in_out_df <- sql(sqlContext, "SELECT * FROM intable i JOIN outtable o ON i.Article = o.link_clicked ORDER BY in_count DESC")
        showDF(in_out_df)
        
        #add  a new ratio column to easily see whether there is more in_count or out_count for an article 
        in_out_ratio_df <- withColumn(in_out_df, "ratio", in_out_df$out_count/in_out_df$in_count)
        in_out_ratio_df2<- select(in_out_ratio_df, "Article","in_count","out_count","ratio")
        showDF(cache(in_out_ratio_df2))
        showDF(in_out_ratio_df2)
        #So, we can infer that 49% of the people who visited the "Fifty Shades of Grey" article clicked on a link in the article and continued to browse wikipedia

#************************************************************************************************************************************************
        
        registerTempTable(in_out_ratio_df, "ratiotable")

#Question 6 -  What does the traffic flow pattern look like for the Deaths_in_2015 article ?

        showDF(sql(sqlContext, "SELECT * FROM ratiotable WHERE Article = 'Deaths_in_2015'"))
        
        #which referers send the most traffic to the Deaths in 2015 article ?
        top_referers_deaths <-  sql(sqlContext, "SELECT * FROM clickstream WHERE curr_title LIKE 'Deaths_in_2015' ORDER BY n DESC LIMIT(10)")            
        showDF(top_referers_deaths)
        
        referers_deaths <- collect(top_referers_deaths)
        
        #And which future articles does the deaths in 2015 article send most traffic onward to?
        top_leavers_deaths <- sql(sqlContext, "SELECT * FROM clickstream WHERE prev_title LIKE 'Deaths_in_2015' ORDER BY n DESC LIMIT(10)")
        showDF(top_leavers_deaths)
        
        leavers_deaths <- collect(top_leavers_deaths)
        
        data_fsog <- rbind(referers_deaths, leavers_deaths)
        
        require(googleVis)
        
        data <- data_fsog[c(-10,-11),c(1,2,3)]
        
        sk1 <- gvisSankey(data, from = "prev_title", to = "curr_title", weight = "n", options=list(width = 1200, height = 500, sankey="{link: { color: { fill: 'lightblue' } }, node: { width: 10, nodePadding: 8, labelPadding: 5, label: { color: 'lightred' }} }"))
        plot(sk1)
        
        data <- data_fsog[,c(1,2)]
        
        d3SimpleNetwork(data, Source = "prev_title", Target = "curr_title", linkDistance = 200, charge = -170, width = 1500, height = 1500, fontsize = 12, textColour = "green", nodeColour = "#3182bd", nodeClickColour = "#E34A33", opacity = 0.5, file = "/Users/rutvikparmar/d3wiki.html")
        
        
#************************************************************************************************************************************************
   

        
        top_leavers <-  sql(sqlContext, "SELECT * FROM clickstream WHERE prev_title IN ('other-wikipedia', 'other-facebook', 'other-twitter', 'other-yahoo', 'other-bing', 'other-google') ORDER BY n DESC LIMIT(30)")
        showDF(top_leavers)
        
        top_leavers_mainpage <- sql(sqlContext, "SELECT * FROM clickstream WHERE prev_title LIKE 'Main_Page' ORDER BY n DESC LIMIT(10)")
        showDF(top_leavers_mainpage)
        
        top_leavers_fiftyshadesofgrey <- sql(sqlContext, "SELECT * FROM clickstream WHERE prev_title LIKE 'Fifty_Shades_of_Grey' ORDER BY n DESC LIMIT(10)")
        showDF(top_leavers_fiftyshadesofgrey)
        
        clickstream <- rbind(top_leavers, top_leavers_mainpage, top_leavers_fiftyshadesofgrey)
        
        data <- collect(clickstream)
     
        clickstream <- data[, c(1,2,3)]
        
        
        head(clickstream)
        tail(clickstream)
        
      
        sk1 <- gvisSankey(clickstream, from = "prev_title", to = "curr_title", weight = "n", options=list(width = 1200, height = 700, sankey="{link: { color: { fill: 'lightblue' } }, node: { width: 10, nodePadding: 8, labelPadding: 5, label: { color: 'lightred' }} }"))
        plot(sk1)
        
        clickstream <- data[, c(1,2)]
        
        d3SimpleNetwork(data, Source = "prev_title", Target = "curr_title", linkDistance = 200, charge = -170, width = 1500, height = 1500, fontsize = 12, textColour = "green", nodeColour = "#3182bd", nodeClickColour = "#E34A33", opacity = 0.5, file = "/Users/rutvikparmar/d3wiki.html")
        
        
sparkR.stop()


