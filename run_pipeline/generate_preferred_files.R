library(data.table)
library(snowfall)

bad_states <- c()#"DE","RI","NH","IA","CT","OK","CO","WA","MI","NC","OH","FL","WI")
desired_min_age <- 45

INPUT_DIR <- "/net/data/twitter-voters/voter-data/ts_chunks/"
OUTPUT_DIR <- "/net/data/twitter-voters/voter-data/preferred_chunks_2/"

dir.create(OUTPUT_DIR, showWarnings = FALSE)


files <- unlist(lapply(state.abb[!state.abb %in% bad_states], 
                       function(l){Sys.glob(file.path(INPUT_DIR,paste0(l,"*")))}))
sfInit(parallel = T,cpus=12)
sfLibrary(data.table)
sfExport("OUTPUT_DIR","desired_min_age")

gen_preferred_chunk <- function(fil){
  d <- fread(fil)
  d <- d[state_count == 1]
  d[,weight:=1]
  d[birth_year <= (2017 - desired_min_age),weight:=weight*8]
  d[! race %in% c("Causcasian","Other","Uncoded","African-American"),weight :=weight *1.5]
  
  # if we have party affiliation
  sampled <- sample(1:nrow(d),300000,prob=d$weight,replace=T)
  
  ## write out fil
  to_write <- d[unique(sampled)]
  to_write$weight <- NULL
  outfile_name <- file.path(OUTPUT_DIR,paste0("preferred_",basename(fil)))
  fwrite(to_write,outfile_name, sep="\t")
  #write.csv(to_write,outfile_name,fileEncoding="utf8")
  return(fil)
}

sfSapply(files,gen_preferred_chunk)
sfStop()
