source("nhl-parse.R")
library(RJSONIO)
library(rmr2)

get.query <- function(query) {
  one <- "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
  two <- "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
  three <- "prefix : <http://www.nhl.com/> "
  conc(one,two,three,query)
}
get.datum <- function(n, dir, year) {
  the.file <- conc(dir,"/",year,"/","file-",n,".json")
  the.value <- fromJSON(the.file)
  the.key <- conc("game-",n)
  keyval(the.key, the.value)

}
get.data <- function(the.range, the.year, the.dir="../data") {
  the.keyvals <- lapply(the.range, function(n) { get.datum(n, the.dir, the.year) } )  
  c.keyval(the.keyvals)
}
