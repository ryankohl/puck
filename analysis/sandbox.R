source("nhlparse.R")
library(RJSONIO)
library(rmr2)
rmr.options(backend= "local")

get.datum <- function(n, dir, year) {
  the.file <- conc(dir,"/",year,"/","file-",n,".json")
  the.value <- fromJSON(the.file)
  the.key <- conc("game-",n)
  keyval(the.key, the.value)

}
get.data <- function() {
  the.range <- 1:10  
  the.dir <- "/Users/ryan/data/nhl"
  the.year <- "2011-2012"
  the.keyvals <- lapply(the.range, function(n) { get.datum(n, the.dir, the.year) } )  
  c.keyval(the.keyvals)
}


hdfs.data= to.dfs(get.data())
result <- mapreduce(
                    input= hdfs.data,
                    map= function(k,v) { game(v) })
ans <- from.dfs(result)

ans$val[2]
