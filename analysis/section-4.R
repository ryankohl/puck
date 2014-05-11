source("definitions.R")
rmr.options(backend= "local")
#quartz()

# We're interested in the first 10 games of the 2011-2012 season
the.range <- 1:10
the.year <- "2011-2012"

# We want to figure out the best team match-ups to watch
matchup.info.q <-
  conc("select ?matchup ?highScorers ?shutouts ?fights ?soloGoals ?hits ",
       " { ?game :matchup ?matchup . ",
       "   optional { ?game :numHighScorers ?highScorers } . ",
       "   optional { ?game :numShutouts ?shutouts } . ",
       "   optional { ?game :numFights ?fights } . ",
       "   optional { ?game :numSoloGoals ?soloGoals } . ",
       "   optional { ?game :numHits ?hits }}")

# Here are the constructs we'll use to extract the req'd info.
team.details.q <-
  conc("construct { ?team a :Team . ?team :nick ?name }  ",
       " { select distinct ?team ?name ",
       "   { { ?game :awayteamid ?team . ?team :nick ?name } union ",
       "     { ?game :hometeamid ?team . ?team :nick ?name  }}}")
player.details.q <-
  conc("construct { ?player a :Player } { ",
       " select distinct ?player { ?player rdfs:label ?name }}")
matchup.q <-
  conc("construct { ?g :matchup ?teams } ",
       " { ?g a :Game . ?x a :Team . ?y a :Team . ",
       "   ?x :nick ?xn . ?y :nick ?yn . ",
       "   filter(?xn < ?yn) . ",
       "   bind(concat(str(?xn),'-',str(?yn)) as ?teams) }")
team.goals.q <-
  conc("construct { ?team :numGoals ?goals }  ",
       " { select ?team (count(?x) as ?goals) ",
       "   { ?team a :Team . ",
       "     optional { ?x a :Goal . ?x :team ?team }} group by ?team }")
player.goals.q <-
  conc("construct { ?player :numGoals ?goals } ",
       " { select ?player (count(?x) as ?goals) ",
       "   { ?player a :Player . ",
       "     optional { ?x a :Goal . ?x :agent1 ?player }} group by ?player }")
high.scorers.q <-
  conc("construct { ?game :numHighScorers ?num } ",
       " { select ?game (count(?player) as ?num) ",
       "   { ?game a :Game . ",
       "     optional { ?player a :Player . ?player :numGoals ?n . filter(?n > 2) }} ",
       "   group by ?game }")
shutout.q <-
  conc("construct { ?game :numShutouts ?num } ",
       " { select ?game (count(?team) as ?num) ",
       "   { ?game a :Game . optional { ?team a :Team . ?team :numGoals 0 }} group by ?game }")
fights.q <-
  conc("construct { ?game :numFights ?num } ",
       " { select ?game (count(?fight) as ?num) ",
       "   { ?game a :Game . optional { ?fight a :Fight } } ",
       "   group by ?game }")
solo.goals.q <-
  conc("construct { ?game :numSoloGoals ?num } ",
       "  { select ?game (count(?goal) as ?num) ",
       "    { ?game a :Game .",
       "      optional { ?goal a :Goal . ?goal :agent2 ?player . ",
       "                 filter(!bound(?player)) }} ",
       "    group by ?game  }")
hits.q <-
  conc("construct { ?game :numHits ?hits } ",
       "  { select ?game (count(?hit) as ?hits) ",
       "    { ?game a :Game . optional { ?hit a :Hit } } group by ?game }")

# We have a few different stages here - first we enhance the game, then mix in
# facts about which resources are teams and players.  Then we mix in on top
# of this facts about the matchups in the games, and goal counts for both
# players and teams.  At this point, we have a baseline model for the extraction
# constructs to follow.
#
# We extract the particular predicates that will end up in our final select, combine
# them together, and finally execute our matchup.info.q query.
do.work <- function(m) {
  enhanced.m <- get.enhanced(m)
  
  teams.m <- construct.rdf(enhanced.m, get.query(team.details.q))
  players.m <- construct.rdf(enhanced.m, get.query(player.details.q))
  enhanced.teams.m <- Reduce(combine.rdf, c(enhanced.m, teams.m, players.m))
  
  matchups.m <- construct.rdf(enhanced.teams.m, get.query(matchup.q))
  player.goals.m <- construct.rdf(enhanced.teams.m, get.query(player.goals.q))
  team.goals.m <- construct.rdf(enhanced.teams.m, get.query(team.goals.q))
  baseline.m <- Reduce(combine.rdf, c(enhanced.teams.m, matchups.m,
                                      player.goals.m, team.goals.m))
  
  high.scorers.m <- construct.rdf(baseline.m, get.query(high.scorers.q))
  shutout.m <- construct.rdf(baseline.m, get.query(shutout.q))
  fights.m <- construct.rdf(baseline.m, get.query(fights.q))
  solo.goals.m <- construct.rdf(baseline.m, get.query(solo.goals.q))
  hits.m <- construct.rdf(baseline.m, get.query(hits.q))
  
  extracted.m <- Reduce(combine.rdf, c(high.scorers.m, shutout.m, fights.m,
                                       solo.goals.m, hits.m, matchups.m))
  the.result <- sparql.rdf(extracted.m, get.query(matchup.info.q))
  if (length(the.result) > 0) { the.result }
}

# As in previous sections, we split up the data frame into the 'teams' (ie matchups) key
# and the stats values, making sure the former is a character vector and the latter is a
# data frame of numeric vectors.
map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  teams <- data.frame(team= as.character(df$matchup), stringsAsFactors=FALSE)
  stats <- data.frame(lapply(df[,2:6], function(m) { as.numeric(as.character(m)) }))
  keyval(teams, stats)
}

# Nothing fancy here - add up all the numeric columns.  There's good reason
# to try alternative combinations, such as an average of values across the games.
# However, I stuck with summing to simplify things.
reduce.job <- function(team, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  keyval(team, roll.up)
}

# We output a  data frame whose first column is a character vector
# of team nicknames, followed by numeric columns for
# number of high scorers (a hat trick or better), shutouts (per side in each game,
# so a 0-0 tie counts as two shutouts), number of fights, the number of goals without
# an assist (seems to always come up zero, so that may be a query mistake), and hit count.
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  r <- data.frame(from.dfs(result))
  names(r) <- c("matchup","high.scorers","shutouts","fights","solo.goals","hits")
  scaled <- apply(r[,-c(1)], 2, function(y) (y - mean(y)) / sd(y) ^ as.logical(sd(y)))*50+100
  s <- data.frame(matchup=r$matchup, scaled)
  r$my.score <- (s$high.scorers*3)+(s$shutouts*5)+(s$fights*10)+(s$solo.goals*7)+(s$hits*9)
  r
}

graph.it <- function(df) {
  odf <- df[order(df$matchup),]
  attach(odf)
  opar <- par(no.readonly=TRUE)
  par(lty=2, pch=16)
  
  dotchart(my.score,
           labels=matchup,
           pch=19)
  
  par(opar)
  detach(odf)
}

# Run against the full dataset, we get too many match-ups.  Let's just see those that
# are outliers - say, with scores over 5000
# x <- run.it()
# graph.it(x[x$my.score > 5000,])
