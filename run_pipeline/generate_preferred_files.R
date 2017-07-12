library(data.table)
library(snowfall)
bad_states <- c()#"DE","RI","NH","IA","CT","OK","CO","WA","MI","NC","OH","FL","WI")
desired_min_age <- 45

DATA_DIR <-  "/net/data/twitter-voters/voter-data/"
INDIR_NAME <- "ts_chunks"
INPUT_DIR <- file.path(DATA_DIR,INDIR_NAME)
PRIOR_NAME <- "preferred_chunks"
PRIOR_GENERATED_DIR <- file.path(DATA_DIR,PRIOR_NAME)
OUTPUT_DIR <- file.path(DATA_DIR,"preferred_chunks_2/")

AGE_SAMPLING_WEIGHTS <- fread("age_resample.csv")


dir.create(OUTPUT_DIR, showWarnings = FALSE)


files <- unlist(lapply(c(state.abb[!state.abb %in% bad_states],"DC"), 
                       function(l){Sys.glob(file.path(INPUT_DIR,paste0(l,"*")))}))

sfInit(parallel = T,cpus=15)
sfLibrary(data.table)
sfExport("OUTPUT_DIR","desired_min_age","AGE_SAMPLING_WEIGHTS","PRIOR_NAME","INDIR_NAME")

gen_preferred_chunk <- function(fil){
  prior_fil <- sub(INDIR_NAME,PRIOR_NAME,fil)
  prior_fil <- sub(basename(prior_fil),paste0("preferred_",basename(prior_fil)),prior_fil)
  d <- fread(fil)
  prior_data <- fread(prior_fil)
  
  # only unique
  d <- d[state_count == 1]
  # ignore prior data
  d <- d[!voter_id %in% prior_data$voter_id]
  
  ### AGE
  # sample by weighting on age
  d[,age := 2017 - birth_year]
  d <- d[age < 100]
  d <- merge(d, AGE_SAMPLING_WEIGHTS,by="age",all.x=T)
  d[,weight:=ifelse(is.na(sample_val),0.0001,sample_val)]
  
  ### GENDER
  # may as well get rid of unknown gender
  # slightly oversample on men
  d[gender == "Male", weight := weight *1.05]
  d[gender == "Unknown", weight := weight *.05]
  
  ### RACE
  # may as well get rid of unknown race
  d[race %in% c("Uncoded","","Other"), weight := weight *.05]
  d[race %in% c("African-American","Asian",
                          "East Asian","Central Asian"), weight := weight * 1.3]

  
  d[party_affiliation %in% c("D","R"), weight := weight * 1.5]
  d[party_affiliation %in% c("","N"), weight := weight * .25]
  
  # if we have party affiliation
  sampled <- sample(1:nrow(d),350000,prob=d$weight,replace=T)
  
  ## write out fil
  to_write <- d[unique(sampled)]
  to_write$weight <- NULL
  to_write$age <- NULL
  to_write$sample_val <- NULL
  to_write$V1 <- NULL
  outfile_name <- file.path(OUTPUT_DIR,paste0("preferred_",basename(fil)))
  fwrite(to_write,outfile_name, sep="\t")
  #write.csv(to_write,outfile_name,fileEncoding="utf8")
  return(fil)
}

sfSapply(files,gen_preferred_chunk)
sfStop()
