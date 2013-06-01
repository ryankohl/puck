source("nhl-data.R")

penaltyDef <- function(pen, match) {
  conc("prefix nhl: <http://www.nhl.com/> ",
       "construct { ?e a nhl:", pen, "} ",
       " { ?e a nhl:Penalty . ?e nhl:desc ?d . FILTER regex(?d, '.*(",
       match,
       ").*')}")
}
penalty.queries <- c(
                     penaltyDef("Boarding", "BOARDING"),
                     penaltyDef("Charging", "CHARGING"),
                     penaltyDef("CrossChecking", "CROSS CHECKING"),
                     penaltyDef("DelayOfGame", "DELAYING GAME"),
                     penaltyDef("Elbowing", "ELBOWING"),
                     penaltyDef("FightingMaj", "FIGHTING (MAJ)"),
                     penaltyDef("HighSticking", "HI-STICKING"),
                     penaltyDef("HighSticking-DoubleMinor", "HI-STICK - DOUBLE MINOR"),
                     penaltyDef("Holding", "HOLDING"),
                     penaltyDef("Hooking", "HOOKING"),
                     penaltyDef("Interference", "INTERFERENCE"),
                     penaltyDef("Misconduct", "MISCONDUCT"),
                     penaltyDef("Roughing", "ROUGHING"),
                     penaltyDef("Slashing", "SLASHING"),
                     penaltyDef("Tripping", "TRIPPING"),
                     penaltyDef("Unsportsmanlike", "UNSPORTSMANLIKE CONDUCT")
              )

the.penalties <- function(the.model) {
  Reduce(combine.rdf, lapply(penalty.queries,
                             function(x) { construct.rdf(the.model, x) }))
}

the.ontology <- load.rdf("../data/ontology/ontology.ttl", format="TURTLE")

get.enhanced <- function(the.model) {
  Reduce(combine.rdf, c(the.penalties(the.model),
                        the.ontology,
                        the.model))
}

get.sample.game <- function(num, year) {
  the.file <- conc("../data", "/",year,"/","file-",num,".json")
  the.json <- fromJSON(the.file)
  get.enhanced(game(the.json))
}

#sample.game <- get.sample.game(1, the.year)
