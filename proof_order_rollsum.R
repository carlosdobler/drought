
library(tidyverse)
library(lmom)


yrs <- 50

wb <- map(seq(12), \(x) rnorm(yrs) |> round(2))
wb <- unlist(wb)
wb <- matrix(wb, yrs, 12)


# APPROACH 1
# rollsum first

wb |> 
  t() |> 
  as.vector() |> 
  plot(type = "l")

wb |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(mean, .before = 5, .complete = T) |>
  plot(type = "l")
  
wb_rolled <- 
  wb |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(mean, .before = 5, .complete = T) |> 
  matrix(11, 12, byrow = T)

wb_rolled <- wb_rolled[-1, ]

params <- 
  wb_rolled |> 
  apply(2, \(x) lmom::pelgno(lmom::samlmu(x)))

wb_perc_1 <- 
  seq(12) |> 
  map(\(mon){
    
    lmom::cdfgno(wb_rolled[,mon], params[,mon])
  
  }) |> 
  unlist() |> 
  matrix(10,12, byrow = F) |> 
  round(2)


# APPROACH 2
# percentiles first, rollsum next
params <- 
  wb[-1,] |> 
  apply(2, \(x) lmom::pelgno(lmom::samlmu(x)))

wb_perc_2 <- 
  seq(12) |> 
  map(\(mon){
    
    lmom::cdfgno(wb[,mon], params[,mon])
    
  }) |> 
  unlist() |> 
  matrix(11,12, byrow = F)

wb_perc_2_rolled <- 
  wb_perc_2 |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(mean, .before = 5, .complete = T) |> 
  matrix(11, 12, byrow = T) |> 
  round(2)

wb_perc_2_rolled <- wb_perc_2_rolled[-1, ]




# APPROACH 3
anom <- 
  wb |> 
  apply(2, \(x){
    
    x-mean(x[-1])
    
  })


anom_rolled <- 
  anom |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(sum, .before = 5, .complete = T) |> 
  matrix(11, 12, byrow = T) %>%
  .[-1,]

anom_rolled |> 
  t() |> 
  as.vector() |> 
  plot(type = "l")

anom_rolled |> 
  apply(2, \(x) ecdf(x)(x)) |> 
  t() |> 
  as.vector() |> 
  plot(type = "l")
  
anom_rolled |> 
  t() |> 
  as.vector() |> 
  


####

anom_perc <- 
  anom |> 
  apply(2, \(x) ecdf(x)(x))


anom_perc |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(sum, .before = 5, .complete = T) |> 
  matrix(11, 12, byrow = T) %>%
  .[-1,] |> 
  t() |> 
  as.vector() |> 
  plot(type = "l")



ecdf(anom_rolled)(anom_rolled) |> 
  t() |> 
  as.vector() |> 
  plot(type = "l")




# ******************

wb |> 
  apply(2, \(x){
    
    # ecdf(x[-1])(x)
    cdfgno(x, pelgno(samlmu(x[-1])))
    
  }) |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(sum, .before = 11, .complete = T) |> 
  plot(type = "l")


wb |> 
  t() |> 
  as.vector() |> 
  slider::slide_dbl(sum, .before = 11, .complete = T) |> 
  matrix(yrs, 12, byrow = T) |> 
  apply(2, \(x) {
    
    ecdf(x[-1])(x)
    # cdfgno(x, pelgno(samlmu(x[-1])))
    
  }) |> 
  t() |> 
  as.vector() |>
  plot(type = "l")
  

  
  
  
  
  
  
  


