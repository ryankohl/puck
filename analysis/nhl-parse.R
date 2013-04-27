source("nhl-utils.R")

game <- function(x) {
  kb <- new.rdf(ontology=FALSE)
  gid <- rdf.blank("game")
  game.data <- x$data$game
  res.fact(kb, gid, ns.rdf("type"), ns.nhl("Game"))
  lit.fact(kb, gid, ns.nhl("awayteamid"),   game.data$awayteamid,   "string")
  lit.fact(kb, gid, ns.nhl("awayteamname"), game.data$awayteamname, "string") 
  lit.fact(kb, gid, ns.nhl("awayteamnick"), game.data$awayteamnick, "string") 
  lit.fact(kb, gid, ns.nhl("hometeamid"),   game.data$hometeamid,   "string") 
  lit.fact(kb, gid, ns.nhl("hometeamname"), game.data$hometeamname, "string") 
  lit.fact(kb, gid, ns.nhl("hometeamnick"), game.data$hometeamnick, "string") 
  play.data <- game.data$plays
  for (p in play.data$play) { play(gid, p, kb) }
  kb
}

play <- function(gid, p, kb) {  
  pid1 <- play.obj(p$pid1, prefix="pid-")
  pid2 <- play.obj(p$pid2, prefix="pid-")
  pid3 <- play.obj(p$pid3, prefix="pid-")
  tid  <- play.obj(p$teamid, prefix="team-")
  eid  <- play.obj(p$formalEventId, prefix="eid-")
  etyp <- play.obj(p$type)
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

