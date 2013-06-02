source("nhl-parse.R")
library(RJSONIO)
library(rmr2)

# wrap a query with our project's namespace prefixes
get.query <- function(query) {
  one <- "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
  two <- "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
  three <- "prefix : <http://www.nhl.com/> "
  conc(one,two,three,query)
}

# get a key-value version of the json file, for map-reduce things
get.datum <- function(n, dir, year) {
  the.file <- conc(dir,"/",year,"/","file-",n,".json")
  the.value <- fromJSON(the.file)
  the.key <- conc("game-",n)
  keyval(the.key, the.value)

}

# go get a bunch of key-value pairs for the json files in the range,
# again for map-reduce things
get.data <- function(the.range, the.year, the.dir="../data") {
  the.keyvals <- lapply(the.range, function(n) { get.datum(n, the.dir, the.year) } )  
  c.keyval(the.keyvals)
}

