source("definitions.R")
rmr.options(backend= "local")
#quartz()

# We're interested in the first 10 games of the 2011-2012 season
the.range <- 1:10
the.year <- "2011-2012"

# We want the numbers of goals, assists, and penalties for every
# player who acted in a game (ie who scored a goal, made an assist,
# or got penalized)
player.info.q <-
  conc("select ?player ?goals ?assists ?penalties ",
       " { ?player a :Player . ",
       "   optional { ?player :numGoals ?goals } . ",
       "   optional { ?player :numAssists ?assists } . ",
       "   optional { ?player :numPenalties ?penalties }}")

# In order to perform the desired select query, we have four
# construct queries to change the shape of the game data,
# extracting the information we need in the form we want
player.goals.q <-
  conc("construct { ?player :numGoals ?goals } { ",
       " { select ?player (count(?x) as ?goals) ",
       "   { ?player a :Player . ",
       "   optional { ?x a :Goal . ?x :agent1 ?player }} group by ?player }}")

player.assists.q <-
  conc("construct { ?player :numAssists ?assists } { ",
       " { select ?player (count(?x) as ?assists) ",
       "  { ?player a :Player . ",
       "    optional { ?x a :Goal . ?x :agent2 ?player } . ",
       "    optional { ?x a :Goal . ?x :agent3 ?player }} group by ?player }}")

player.penalties.q <-
  conc("construct { ?player :numPenalties ?penalties } { ",
       " { select ?player (count(?x) as ?penalties) ",
       "   { ?player a :Player . ",
       "     optional { ?x a :Penalty . ?x :agent1 ?player }} group by ?player }}")

player.details.q <-
  conc("construct { ?player a :Player } { ",
       " select distinct ?player { ?player rdfs:label ?name }}")

# Here's where all the semantic work is done.  A game is enhanced with
# the ontology and construct queries in definitions.R.

# We then add in the extracted facts of which entites are players (turns
# out it's just the things with rdfs:label facts, according to our nhl-parse.R)

# Then we extract three models out - goal, assist, and penalty counts for each player

# Finally, we execute the player.info.q query against the combination of the extracts,
# which again includes the details.m model.  Note that we're leaving behind the
# enhanced game, and only combining the pieces we need into the model that gets
# queried in the end.
do.work <- function(m) {
  preEnhanced.game <- get.enhanced(m)
  details.m <- construct.rdf(preEnhanced.game, get.query(player.details.q))
  enhanced.game <- combine.rdf(preEnhanced.game, details.m)
  
  goals.m <- construct.rdf(enhanced.game, get.query(player.goals.q))
  assists.m <- construct.rdf(enhanced.game, get.query(player.assists.q))
  penalties.m <- construct.rdf(enhanced.game, get.query(player.penalties.q))
  
  extracted.m <- Reduce(combine.rdf, c(goals.m, assists.m, penalties.m, details.m))
  the.result <- sparql.rdf(extracted.m, get.query(player.info.q)) 
  if (length(the.result) > 0) { the.result }
}

# The map job gets a data.frame from the semantic work above, pulls out the
# players vector as a character vector, and turns the remaining columns into
# numeric vectors.  The players are our keys, and the values are our numeric
# stats.  Since we didn't mess with the row order, these keys and values will
# line up (ie. key 1 matches value 1, key 52 matches value 52, etc)
map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  players <- data.frame(player= as.character(df$player), stringsAsFactors=FALSE)
  stats <- data.frame(lapply(df[,2:4], function(m) { as.numeric(as.character(m)) }))
  keyval(players, stats)
}

# When the reduce job gets called, it has a player and all the stats from each
# of the games ( in a data frame resulting from a combination [rbind] of each of
# the products of the map)
# We want to roll up the values in this data frame, adding up all the goals, the
# assists, and the penalties.  So we use colSums to do the summation and bind it
# all up into a data frame.  This is a good place to calculate the points for the
# player, which is twice the number of goals plus the number of assists.  All of this
# is then combined into a single data frame (info) and shuttled off, with the players
# as the key.
reduce.job <- function(player, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  p <- 2*roll.up$goals+roll.up$assists
  info <- data.frame(roll.up, points= p)
  keyval(player, info)
}

# Here's the execution of the map-reduce job, which outputs a data frame with the
# first column for players, followed by numeric columns for sums of goals, assists,
# penalties, and points for the games in the data.
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  r <- data.frame(from.dfs(result))
  names(r) <- c("player","goals","assists","penalties","points" )
  r
}

# Let's take a look at the relationships between some of our
# variables all at once in a 2x2 graph box.
graph.it <- function(df) {
  attach(df)
  opar <- par(no.readonly=TRUE)

  par(lty=2, pch= 16, mfrow=c(2,2))
  plot(goals, assists)
  plot(goals, penalties)
  plot(goals, points)
  plot(assists, penalties)
  
  par(opar)
  detach(df)
}
