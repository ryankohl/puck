source("nhl-data.R")
source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10
the.year <- "2011-2012"

player.info.q <-
  conc("select ?player ?goals ?assists ?penalties ",
       " { ?player a :Player . ",
       "   optional { ?player :numGoals ?goals } . ",
       "   optional { ?player :numAssists ?assists } . ",
       "   optional { ?player :numPenalties ?penalties }}")

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
map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  players <- data.frame(player= as.character(df$player))
  stats <- data.frame(lapply(df[,2:4], function(m) { as.numeric(as.character(m)) }))
  keyval(players, stats)
}
reduce.job <- function(player, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  p <- 2*roll.up$goals+roll.up$assists
  info <- data.frame(roll.up, points= p)
  keyval(player, info)
}
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  data.frame(from.dfs(result))
}
