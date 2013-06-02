source("definitions.R")
rmr.options(backend= "local")

# We're interested in the first 10 games of the 2011-2012 season
the.range <- 1:10
the.year <- "2011-2012"

# We want the number of goals, assists, and penalties for each team
team.info.q <-
  conc("select ?team ?goals ?assists ?penalties ",
       " { ?t a :Team . ?t :nick ?team",
       "   optional { ?t :numGoals ?goals } . ",
       "   optional { ?t :numAssists ?assists } . ",
       "   optional { ?t :numPenalties ?penalties }}")

# In order to perform our select query, we're going to use four construct
# queries on each game to extract the needed info.  The trick here is to
# make sure we're distinguishing home team from away team events.
team.goals.q <-
  conc("construct { ?team :numGoals ?goals } { ",
       " { select ?team (count(?x) as ?goals) ",
       "   { ?team a :Team . ",
       "   optional { ?x a :Goal . ?x :team ?team }} group by ?team }}")

team.assists.q <-
  conc("construct { ?team :numAssists ?assists } { ",
       " { select ?team (count(?x) as ?assists) ",
       "  { ?team a :Team . ",
       "    optional { ?x a :Goal . ?x :team ?team }} group by ?team }}")

team.penalties.q <-
  conc("construct { ?team :numPenalties ?penalties } { ",
       " { select ?team (count(?x) as ?penalties) ",
       "   { ?team a :Team . ",
       "     optional { ?x a :Penalty . ?x :team ?team }} group by ?team }}")

team.details.q <-
  conc("construct { ?team a :Team . ?team :nick ?name } { ",
       " select distinct ?team ?name {{ ?game :awayteamid ?team . ?team :nick ?name } union ",
       "                        { ?game :hometeamid ?team . ?team :nick ?name  }}}")

# Our semantic work starts off by mixing in the ontology and construct queries
# from definitions.R.  We then extract and mix in the team.details.q, so that we
# know which resources are teams, and what their nicknames are.

# We then extract the goals, assists, and penalties for each team from the game,
# combining these facts with the extracted facts from the team.details.q, resulting
# in a model that only contains the facts we need for the ultimate team.info.q query.
do.work <- function(m) {
  preEnhanced.game <- get.enhanced(m)
  details.m <- construct.rdf(preEnhanced.game, get.query(team.details.q))
  enhanced.game <- combine.rdf(preEnhanced.game, details.m)
  
  goals.m <- construct.rdf(enhanced.game, get.query(team.goals.q))
  assists.m <- construct.rdf(enhanced.game, get.query(team.assists.q))
  penalties.m <- construct.rdf(enhanced.game, get.query(team.penalties.q))
  
  extracted.m <- Reduce(combine.rdf, c(goals.m, assists.m, penalties.m, details.m))
  the.result <- sparql.rdf(extracted.m, get.query(team.info.q)) 
  if (length(the.result) > 0) { the.result }
}

# Our map job takes the results of the semantic work, turns it into a data frame,
# and then splits apart the teams column as a character vector, leaving the rest of
# the data frame to be converted to numeric vectors.  The teams vector contains our keys,
# while the numeric vectors conveying team stats are combined into a data frame for our values.
map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  teams <- data.frame(team= as.character(df$team), stringsAsFactors=FALSE)
  stats <- data.frame(lapply(df[,2:4], function(m) { as.numeric(as.character(m)) }))
  keyval(teams, stats)
}

# Similar to the section-2 exercise, we'll be summing up the values of each column in the
# stats data frame with colSums, turning the result back into a data frame.  At this point
# we calclulate the points (twice the number of goals, plus the number of assists) for the team
# and add that to the resulting data frame.
reduce.job <- function(team, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  p <- 2*roll.up$goals+roll.up$assists
  info <- data.frame(roll.up, points= p)
  keyval(team, info)
}

# This is the execution of our map-reduce job, which outputs a data frame whose first
# column is a character vector of team nicknames, followed by numeric columns for goals,
# assists, penalties, and points.
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  data.frame(from.dfs(result))
}
