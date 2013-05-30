source("nhl-data.R")
source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10
the.year <- "2011-2012"

team.info.q <-
  conc("select ?team ?goals ?assists ?penalties ",
       " { ?t a :Team . ?t :nick ?team",
       "   optional { ?t :numGoals ?goals } . ",
       "   optional { ?t :numAssists ?assists } . ",
       "   optional { ?t :numPenalties ?penalties }}")

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
map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  teams <- data.frame(team= as.character(df$team))
  stats <- data.frame(lapply(df[,2:4], function(m) { as.numeric(as.character(m)) }))
  keyval(teams, stats)
}
reduce.job <- function(team, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  p <- 2*roll.up$goals+roll.up$assists
  info <- data.frame(roll.up, points= p)
  keyval(team, info)
}
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  data.frame(from.dfs(result))
}
get.sample <- function(num, year) {
  the.file <- conc("../data", "/",year,"/","file-",num,".json")
  the.json <- fromJSON(the.file)
  get.enhanced(game(the.json))
}

sample.game <- get.sample(1, the.year)
