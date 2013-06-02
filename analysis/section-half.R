source("nhl-data.R")
library("RJSONIO")
# let's start off with a knowledge base we filled up with
# our base parsing functions (in nhl-parse.R)
the.file <- "../data/2011-2012/file-1.json"
the.json <- fromJSON(the.file)
the.game <- game(the.json)

# We'll reuse the survey query from section-0 to see what we have,
# but wrap it up into a function to make this easy
survey <- function(kb) {
  survey.q <-
    conc("prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ",
         "select ?class ?pred (count(*) as ?cnt) ",
         "{ ?subj a ?class . ?subj ?pred ?obj } ",
         "group by ?class ?pred ",
         "order by ?class ?cnt")
  survey.r <- data.frame(sparql.rdf(kb, survey.q))
  survey.r$class <- gsub("http://www.nhl.com/",":",survey.r$class,fixed=TRUE)
  survey.r$class <- gsub("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf:",
                        survey.r$class,fixed=TRUE)
  survey.r$pred <- gsub("http://www.nhl.com/",":",survey.r$pred,fixed=TRUE)
  survey.r$pred <- gsub("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf:",
                        survey.r$pred,fixed=TRUE)
  survey.r
}

survey(the.game)

# We'd like to add some additional information to the base
# More specifically, there's a bunch of information we
# can add about penalties - an integral part of any hockey game

# We're going to start by taking an ontology stored off in a Turtle
# file (../data/ontology/ontology.ttl) and mix it into our kb
the.ontology <- load.rdf("../data/ontology/ontology.ttl", format="TURTLE")
ontologized.game <- combine.rdf(the.game, the.ontology)

# Okay, what do we have now?
survey(ontologized.game)

# Hmmm, it'd be nice if we actually had the specific classes for individual
# penalties in the game - the ontology's not doing much for us right now.
# We'll use a construct query and mix it in to the ontologized game
holding.q <- conc("prefix nhl: <http://www.nhl.com/> ",
                  "construct { ?e a nhl:Holding } ",
                  " { ?e a nhl:Penalty . ?e nhl:desc ?d . ",
                  "   filter regex(?d, '.*(HOLDING).*')}")
holding.m <- construct.rdf(ontologized.game, holding.q)
combined.game <- combine.rdf(ontologized.game, holding.m)

# survey(combined.game)

# Looks good!  But that's a lot of work for every kind of penalty... let's
# use some functions to make this easier.

# First, we'll make a function that takes a class name (pen) we want to use
# and a text match (from the :desc value of penalties) and build up the
# construct
penaltyDef <- function(pen, match) {
  conc("prefix nhl: <http://www.nhl.com/> ",
       "construct { ?e a nhl:", pen, "} ",
       " { ?e a nhl:Penalty . ?e nhl:desc ?d . FILTER regex(?d, '.*(",
       match,
       ").*')}")
}

# So it looks like this:
# penaltyDef("Holding", "HOLDING")

# Great!  Now let's define those pairs we'll use:
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

# So now we have a list of construct queries.  Let's use some fancy
# functional programming - lapply to execute each of these constructs
# against our ontologized.game, and a reduce to combine 'em all together
# We'll start over with our original 'the.game' - the initial RDF parse product
penalty.m <- Reduce(combine.rdf, lapply(penalty.queries,
                                        function(x) { construct.rdf(the.game, x) }))

# Let's now put it all together into a baseline model
enhanced.game <- Reduce(combine.rdf, c(penalty.m, the.ontology, the.game))

# What do we have?
# survey(enhanced.game)

# That's what we want - remember the ontology tied penalty minutes to the
# penalty classes (that's why you see it attached to rdfs:Resource in the
# survey query).  
                        

