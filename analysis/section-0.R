source("nhl-utils.R")

the.file <- "../data/2011-2012/file-1.json"
the.json <- fromJSON(the.file)

kb <- new.rdf(ontology=FALSE)

gid <- rdf.blank("game")

# names(the.json)
# names(the.json$data)
# names(the.json$data$game)

game.data <- the.json$data$game

# backslashes are terrible things, so let's
# get them out of the way when declaring
# resource URIs.  So we end up here
# with a good URI for a resource, with
# an optional prefix
good.obj <- function(p, prefix="") {
  subbed <- gsub("\\","",p,fixed=TRUE)
  ns.nhl(conc(prefix, subbed))
}

# home and away teams
aid <- play.obj(game.data$awayteamid, prefix="team-")
hid <- play.obj(game.data$hometeamid, prefix="team-")

# names(game)

# let's declare our basic resources
res.fact(kb, gid, ns.rdf("type"), ns.nhl("Game"))
res.fact(kb, gid, ns.nhl("awayteamid"),   aid)
res.fact(kb, gid, ns.nhl("hometeamid"),   hid)

# and now some facts about them
lit.fact(kb, aid, ns.nhl("name"), game.data$awayteamname, "string") 
lit.fact(kb, aid, ns.nhl("nick"), game.data$awayteamnick, "string")
lit.fact(kb, hid, ns.nhl("name"), game.data$hometeamname, "string") 
lit.fact(kb, hid, ns.nhl("nick"), game.data$hometeamnick, "string")

# finally, we need to rip through our plays
play.data <- game.data$plays
# str(play.data)
# names(play.data$play[[1]])

# okay, now we have some meat to eat
for (p in play.data$play) {
  # resources first
  tid  <- play.obj(p$teamid, prefix="team-")
  eid  <- play.obj(p$formalEventId, prefix="eid-")
  etyp <- play.obj(p$type)
  pid1 <- play.obj(p$pid1, prefix="pid-")
  pid2 <- play.obj(p$pid2, prefix="pid-")
  pid3 <- play.obj(p$pid3, prefix="pid-")

  # then resource facts
  res.fact(kb, gid, ns.nhl("play"), eid)
  res.fact(kb, eid, ns.rdf("type"), etyp)
  res.fact(kb, eid, ns.nhl("agent1"), pid1)
  res.fact(kb, eid, ns.nhl("agent2"), pid2)
  res.fact(kb, eid, ns.nhl("agent3"), pid3)
  res.fact(kb, eid, ns.nhl("team"), tid)

  # finally some literal facts
  lit.fact(kb, eid, ns.nhl("desc"), p$desc, "string")
  lit.fact(kb, pid1, ns.rdfs("label"), p$p1name, "string")
  lit.fact(kb, pid2, ns.rdfs("label"), p$p2name, "string")
  lit.fact(kb, pid3, ns.rdfs("label"), p$p3name, "string")
  lit.fact(kb, eid, ns.nhl("time"), conc("00:",p$time), "time")
  lit.fact(kb, eid, ns.nhl("period"), p$period, "integer")
}

# so what do we have in our kb now?
survey.q <-
  conc("prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ",
       "select ?class ?pred (count(*) as ?cnt) ",
       "{ ?subj a ?class . ?subj ?pred ?obj } ",
       "group by ?class ?pred ",
       "order by ?class ?cnt")
survey.r <- sparql.rdf(kb, survey.q)
# survey.r
# class(survey.r)
survey.r <- data.frame(survey.r)
# survey.r

# oh god, those namespaces are killing me.  let's clean up the data frame
survey.r$class <- gsub("http://www.nhl.com/",":",survey.r$class,fixed=TRUE)
survey.r$pred <- gsub("http://www.nhl.com/",":",survey.r$pred,fixed=TRUE)
survey.r$pred <- gsub("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf:",
                      survey.r$pred,fixed=TRUE)
