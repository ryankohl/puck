library("rrdf")

conc <- function(...) { paste( list(...), collapse="" ) }
ns.rdf <- function(x) { conc("http://www.w3.org/1999/02/22-rdf-syntax-ns#",x) }
ns.rdfs <- function(x) { conc("http://www.w3.org/2000/01/rdf-schema#",x) }
ns.nhl <- function(x) { conc("http://www.nhl.com/",x) }

get.query <- function(query) {
  one <- "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
  two <- "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
  three <- "prefix : <http://www.nhl.com/> "
  conc(one,two,three,query)
}

is.nil <- function(x) { x == '' || is.null(x) }
good.fact <- function(x) { length(x) == 3 && !(TRUE %in% sapply(x, is.nil)) }

res.fact <- function(kb,s,p,o) {
  if (good.fact(c(s,p,o))) { add.triple(kb, subject=s, predicate=p, object=o) }
}
lit.fact <- function(kb,s,p,o,t) {
  if (good.fact(c(s,p,o))) { add.data.triple(kb, subject=s, predicate=p, data=as.character(o), type=t)}
}

play.obj <- function(x, prefix="") {
  subbed <- gsub("\\","",x,fixed=TRUE)
  ns.nhl(conc(prefix, subbed))
}

play <- function(gid, x, kb) {  
  pid1 <- play.obj(x$pid1, prefix="pid-")
  pid2 <- play.obj(x$pid2, prefix="pid-")
  pid3 <- play.obj(x$pid3, prefix="pid-")
  tid  <- play.obj(x$teamid, prefix="team-")
  eid  <- play.obj(x$formalEventId, prefix="eid-")
  etyp <- play.obj(x$type)
  res.fact(kb, gid, ns.nhl("play"), eid)
  res.fact(kb, eid, ns.rdf("type"), etyp)
  lit.fact(kb, eid, ns.nhl("desc"), x$desc, "string")
  res.fact(kb, eid, ns.nhl("agent1"), pid1)
  res.fact(kb, eid, ns.nhl("agent2"), pid2)
  res.fact(kb, eid, ns.nhl("agent3"), pid3)
  lit.fact(kb, pid1, ns.rdfs("label"), x$p1name, "string")
  lit.fact(kb, pid2, ns.rdfs("label"), x$p2name, "string")
  lit.fact(kb, pid3, ns.rdfs("label"), x$p3name, "string")
  res.fact(kb, eid, ns.nhl("team"), tid)
  lit.fact(kb, eid, ns.nhl("lat"), x$xcoord, "string")
  lit.fact(kb, eid, ns.nhl("lon"), x$ycoord, "string")
  lit.fact(kb, eid, ns.nhl("time"), conc("00:",x$time), "time")
  lit.fact(kb, eid, ns.nhl("period"), x$period, "integer")
}
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
