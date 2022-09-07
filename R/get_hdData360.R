
get_hdData360 <- function(firstYear = "mandatory {1970,...,'actual year'}", impute = "optional {0,...,30}", writeTSV = "optional {FALSE,TRUE}"){
  
  options(warn=-1)
  
  pkgs <- c("lubridate", "data360r", "stringr", "dplyr", "zoo")
  if(length (pkgs <- setdiff(pkgs, rownames(installed.packages()))))install.packages(pkgs)
  rm(pkgs)
  
  library(lubridate)
  library(data360r)
  library(stringr)
  library(dplyr)
  library(zoo)
  
  if (firstYear < 1970 | firstYear > lubridate::year(Sys.Date())) stop("Invalid 'firstYear' argument!")
  if (missing("impute")){impute = 0}
  if (impute < 0 | impute > 30) stop("Invalid 'impute' argument!")
  if (missing("writeTSV")){writeTSV = FALSE}
  
  ################################################################
  # get metadata
  ################################
  
  site <- c("gov","tc")
  
  for(i in 1:length(site)){
    
    meta <- data360r::get_metadata360(site = site[i], metadata_type = "indicators")
    meta <- meta %>% dplyr::filter(!grepl("monthly|quarterly|every Infinity years", periodicity))
    
    years <- str_sub(meta$dateRange, -4, -1)
    id <- which(stringr::str_sub(years, -2, -2) == "Q")
    
    if(length(id) != 0){
      years <- years[-id]
      meta <- meta[-id,] 
    }
    
    meta <- meta[as.numeric(years) >= firstYear,]
    meta[,ncol(meta)+1] <- site[i]
    colnames(meta)[ncol(meta)] <- "site"
    
    if(i == 1){
      out <- meta
    }else{
      out <- rbind(out, meta)
    }
    
  }
  
  meta <- out %>% dplyr::distinct(name, dataset, .keep_all = TRUE)

  rm(list=setdiff(ls(), c("meta", "firstYear", "impute", "writeTSV", "site")))
  
  ################################################################
  # get data
  ################################  
  
  meta <- meta[meta$byProduct==FALSE,]
  meta <- meta[meta$byPartner==FALSE,]
  ctry <- data360r::get_metadata360(site = "gov", metadata_type = "countries")
  
  out <- as.data.frame(sort(unique(ctry$iso3)))
  colnames(out) <- "iso3"
  
  for(i in 1:length(site)){
    
    gc()
    meta_site <- meta[meta$site==site[i],]
    id <- meta_site$id
    
    for(j in 1:length(id)){
      
      try(dt <- as.data.frame(data360r::get_data360(site = site[i], indicator_id = id[j], output_type = "wide")))      
      try(colnames(dt) <- stringr::str_sub(colnames(dt), -4, -1))
      
      try(tmet <- meta_site[meta_site$id==id[j],])
      try(yr <- unlist(tmet$timeframes))
      try(yr <- as.numeric(stringr::str_sub(yr, -4, -1)))
      
      try(yr1 <- max(min(yr), firstYear-impute))
      try(yr2 <- max(yr))
      
      try(dt <- dt %>% dplyr::select(1,matches(as.character(yr1:yr2))))
      try(colnames(dt) <- append("iso3", paste(id[j], colnames(dt)[2:ncol(dt)], sep="@")))
      try(out <- merge(x = out, y = dt, by = "iso3", all.x = TRUE))
      
      print(paste(site[i],"Data360: ",sprintf('%.2f',j/length(id)*100),"% is ready",sep=""))
      
    }
    
  }
  
  dt <- out
  
  rm(list=setdiff(ls(), c("dt", "ctry", "meta", "firstYear", "impute", "writeTSV")))
  
  ################################################################
  # missing data imputation
  ################################
  
  if (!impute==0){
  
    id1 <- sub("\\@.*", "", colnames(dt))
    id2 <- unique(id1[2:length(id1)])
    
    yr <- as.numeric(sub(".*@", "", colnames(dt)[2:ncol(dt)]))
    dt <- dt[,which(append(TRUE,!yr>lubridate::year(Sys.Date()))==TRUE)]
    yr <- as.numeric(sub(".*@", "", colnames(dt)[2:ncol(dt)]))
    
    yr_min <- min(yr, na.rm = TRUE)
    yr_max <- max(yr, na.rm = TRUE)
    
    all <- apply(expand.grid(id2, as.character(yr_min:yr_max)), 1, paste, collapse="@")
    new <- setdiff(all, colnames(dt)[2:ncol(dt)])
    emp <- as.data.frame(matrix(NA,nrow(dt), length(new)))
    colnames(emp) <- new
    
    dt <- cbind(dt, emp)
    dt <- dt[,order(colnames(dt))]
    
    id1 <- sub("\\@.*", "", colnames(dt))
    id2 <- unique(id1[2:length(id1)])
    
    res <- dt
    
    for(i in 2:length(id2)){
      
      id <- which(id1 == id2[i])
      ms <- min(length(id), impute+1)
      ln <- length(id)
      
      for(j in ms:ln){
        
          if(j==ms){
            
            idn <- id[max((j-impute),1):j]
            
            tmp <- as.data.frame(t(na.locf(t(dt[,idn]))))
            tmp <- tmp[, ncol(tmp), drop = FALSE]
            res[,ncol(res)+ncol(tmp)] <- tmp[,ncol(tmp), drop = FALSE]
            
          }else{
             
            idn <- id[max((j-impute),1):j]
            
            tmp <- as.data.frame(t(na.locf(t(dt[,idn]))))
            tmp <- tmp[, ncol(tmp), drop = FALSE]
            res[,ncol(res)+ncol(tmp)] <- tmp[, ncol(tmp), drop = FALSE]
            
          }
        
      }
     
      print(paste("imputation: ",sprintf('%.2f',i/length(id2)*100),"% is ready",sep=""))

    }
    
  }else{
    
    res <- dt
  
  }
  
  gen <- as.data.frame(Sys.time())
  gen[,2] <- firstYear
  gen[,3] <- impute
  colnames(gen) <- c("timestamp","firstYear","impute")
  
  out <- list()
  out[[1]] <- cbind(as.data.frame(ctry[,1, drop = FALSE]),res)
  out[[2]] <- meta
  out[[3]] <- ctry
  out[[4]] <- gen
  
  names(out) <- c("data","meta","ctry","info")
  out$meta <- out$meta[,-c(9,14,15)]
  out$ctry <- out$ctry[,-11]
  colnames(out$data)[1] <- "iso3"
  
  if (writeTSV == TRUE){
    
    dir <- paste("hdData360r__",firstYear,"_",impute,"_",date(out$info$timestamp),sep="")
    outDir <- file.path(".", dir)
    if (!dir.exists(outDir)) {dir.create(outDir)}

    meta <- out$meta[,-c(8,9,12)]
    write.table(out$data,paste("./",dir,"/data.tsv",sep=""),sep="\t",row.names = FALSE)
    write.table(meta,paste("./",dir,"/meta.tsv",sep=""),sep="\t",row.names = FALSE)
    write.table(out$ctry,paste("./",dir,"/ctry.tsv",sep=""),sep="\t",row.names = FALSE)
    write.table(out$info,paste("./",dir,"/info.tsv",sep=""),sep="\t",row.names = FALSE)
    rm(meta)

  }

  options(warn=0)
  
  return(out)

}
