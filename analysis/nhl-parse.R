source("nhl-utils.R")
library(RJSONIO)

# parse a game node, given a parsed JSON object,
# and return an RDF model with the results
game <- function(x) {
  # start off with an RDF model that has OWL inference off
  kb <- new.rdf(ontology=FALSE)

  # declare the resources we'll use in the parse
  gid <- rdf.blank("game")
  game.data <- x$data$game
  aid <- play.obj(game.data$awayteamid, prefix="team-")
  hid <- play.obj(game.data$hometeamid, prefix="team-")

  # assert high-level game facts to the kb
  res.fact(kb, gid, ns.rdf("type"), ns.nhl("Game"))
  res.fact(kb, gid, ns.nhl("awayteamid"),   aid)
  lit.fact(kb, aid, ns.nhl("name"), game.data$awayteamname, "string") 
  lit.fact(kb, aid, ns.nhl("nick"), game.data$awayteamnick, "string") 
  res.fact(kb, gid, ns.nhl("hometeamid"),   hid) 
  lit.fact(kb, hid, ns.nhl("name"), game.data$hometeamname, "string") 
  lit.fact(kb, hid, ns.nhl("nick"), game.data$hometeamnick, "string")

  # for each play, add it to the kb
  play.data <- game.data$plays
  for (p in play.data$play) { play(gid, p, kb) }
  kb
}

# parse a play node, given a parsed JSON object,
# and put it in the supplied RDF model (kb)
play <- function(gid, p, kb) {
  # the resources we'll use in parsing the play
  pid1 <- play.obj(p$pid1, prefix="pid-")
  pid2 <- play.obj(p$pid2, prefix="pid-")
  pid3 <- play.obj(p$pid3, prefix="pid-")
  tid  <- play.obj(p$teamid, prefix="team-")
  eid  <- play.obj(p$formalEventId, prefix="eid-")
  etyp <- play.obj(p$type)

  # the facts for the play we'll be asserting
  res.fact(kb, gid, ns.nhl("play"), eid)
  res.fact(kb, eid, ns.rdf("type"), etyp)
  lit.fact(kb, eid, ns.nhl("desc"), p$desc, "string")
  res.fact(kb, eid, ns.nhl("agent1"), pid1)
  res.fact(kb, eid, ns.nhl("agent2"), pid2)
  res.fact(kb, eid, ns.nhl("agent3"), pid3)
  lit.fact(kb, pid1, ns.rdfs("label"), p$p1name, "string")
  lit.fact(kb, pid2, ns.rdfs("label"), p$p2name, "string")
  lit.fact(kb, pid3, ns.rdfs("label"), p$p3name, "string")
  res.fact(kb, eid, ns.nhl("team"), tid)
  lit.fact(kb, eid, ns.nhl("lat"), p$xcoord, "string")
  lit.fact(kb, eid, ns.nhl("lon"), p$ycoord, "string")
  lit.fact(kb, eid, ns.nhl("time"), conc("00:",p$time), "time")
  lit.fact(kb, eid, ns.nhl("period"), p$period, "integer")
}

# removes any backslashes from the play identifier
# before using it as a uri in a resource
play.obj <- function(p, prefix="") {
  subbed <- gsub("\\","",p,fixed=TRUE)
  ns.nhl(conc(prefix, subbed))
}

# get an RDF model for a game
get.sample <- function(num, year) {
  # we get the path to the file
  the.file <- conc("../data", "/",year,"/","file-",num,".json")
  # we turn it into an R list after reading in the JSON
  the.json <- fromJSON(the.file)
  # we turn the R-list-JSON into RDF
  game(the.json)
}

# to investigate the R-list-JSON, use names(x) to see
# what nodes you have available
