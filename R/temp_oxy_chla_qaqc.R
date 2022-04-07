# Install packages
pacman::p_load(tidyverse,lubridate, plotly, dplyr,stringr)

temp_oxy_chla_qaqc <- function(realtime_file,
                               qaqc_file,
                               maintenance_file,
                               input_file_tz,
                               focal_depths,
                               config){

  #####################################################################################################################
  # QAQC data from the data logger at FCR based on what is published on EDI

  CATDATA_COL_NAMES <- c("DateTime", "RECORD", "CR6_Batt_V", "CR6Panel_Temp_C", "ThermistorTemp_C_surface",
                         "ThermistorTemp_C_1", "ThermistorTemp_C_2", "ThermistorTemp_C_3", "ThermistorTemp_C_4",
                         "ThermistorTemp_C_5", "ThermistorTemp_C_6", "ThermistorTemp_C_7", "ThermistorTemp_C_8",
                         "ThermistorTemp_C_9", "RDO_mgL_5", "RDOsat_percent_5", "RDOTemp_C_5", "RDO_mgL_9",
                         "RDOsat_percent_9", "RDOTemp_C_9", "EXO_Date", "EXO_Time", "EXOTemp_C_1", "EXOCond_uScm_1",
                         "EXOSpCond_uScm_1", "EXOTDS_mgL_1", "EXODOsat_percent_1", "EXODO_mgL_1", "EXOChla_RFU_1",
                         "EXOChla_ugL_1", "EXOBGAPC_RFU_1", "EXOBGAPC_ugL_1", "EXOfDOM_RFU_1", "EXOfDOM_QSU_1",
                         "EXO_pressure_psi", "EXO_depth_m", "EXO_battery_V", "EXO_cablepower_V", "EXO_wiper_V", "Lvl_psi_9", "LvlTemp_C_9")


  # EXO sonde sensor data that differs from the mean by more than the standard deviation multiplied by this factor will
  # either be replaced with NA and flagged (if between 2018-10-01 and 2019-03-01) or just flagged (otherwise)
  EXO_FOULING_FACTOR <- 4

  #Adjustment period of time to stabilization after cleaning DO sensor. Ajustement period is 2 hours in seconds
  ADJ_PERIOD = 2*60*60

  # read catwalk data and maintenance log
  # NOTE: date-times throughout this script are processed as UTC
  catdata <- readr::read_csv(realtime_file, skip = 4, col_names = CATDATA_COL_NAMES,
                             col_types = readr::cols(.default = readr::col_double(), DateTime = readr::col_datetime()))

  log <- readr::read_csv(maintenance_file, col_types = readr::cols(
    .default = readr::col_character(),
    TIMESTAMP_start = readr::col_datetime("%Y-%m-%d %H:%M:%S%*"),
    TIMESTAMP_end = readr::col_datetime("%Y-%m-%d %H:%M:%S%*"),
    flag = readr::col_integer()
  ))

  ####################################################################################################################################
  # Quinn you can determine if you need this because if you are taking data from EDI for then you do not need this
  # but if you are just pulling in the data from the data logger then you need to change the dates when the tz on the
  # data logger was changed

  #Fix the timezone issues
  #time was changed from GMT-4 to GMT-5 on 15 APR 19 at 10:00
  #have to seperate data frame by year and record because when the time was changed 10:00-10:40 were recorded twice
  #once before the time change and once after so have to seperate and assign the right time.
  before=catdata%>%
    dplyr::filter(DateTime<"2019-04-15 6:50")%>%
    dplyr::filter(DateTime<"2019-04-15 6:50" & RECORD < 32879)#Don't know how to change timezones so just subtract 4 from the time we want


  #now put into GMT-5 from GMT-4
  before$DateTime<-as.POSIXct(strptime(before$DateTime, "%Y-%m-%d %H:%M"), tz = "Etc/GMT+5") #get dates aligned
  before$DateTime<-lubridate::with_tz(force_tz(before$DateTime,"Etc/GMT+4"), "Etc/GMT+5") #pre time change data gets assigned proper timezone then corrected to GMT -5 to match the rest of the data set


  #filter after the time change
  after=catdata%>%
    dplyr::filter(DateTime>"2019-04-15 05:50")%>%
    dplyr::slice(-c(1,3,5,7,9))


  after$DateTime<-as.POSIXct(strptime(after$DateTime, "%Y-%m-%d %H:%M"), tz = "Etc/GMT+5")#change or else after is off by an hour



  #merge before and after so they are one dataframe in GMT-5

  catdata=rbind(before, after)

  catdata=catdata[!duplicated(catdata$DateTime), ]

  catdata$DateTime<-as.POSIXct(strptime(catdata$DateTime, "%Y-%m-%d %H:%M:%S"), tz = "UTC")
  ###############################################################################################################################

  # remove NaN data at beginning
  catdata <- catdata%>% dplyr::filter(DateTime >= lubridate::ymd_hms("2018-07-05 14:50:00"))

  #for loop to replace negative values with 0

  for(k in c(15:16,18:19,24:32)) {
    catdata[c(which((catdata[,k]<0))),k] <- 0
  }

  #add in the adjusted DO columns for used below as dates are in the maintenance log
  catdata$RDO_mgL_5_adjusted <-0
  catdata$RDOsat_percent_5_adjusted <-0
  catdata$RDO_mgL_9_adjusted <-0
  catdata$RDOsat_percent_9_adjusted <-0



  # modify catdata based on the information in the log
  for(i in 1:nrow(log))
  {
    # get start and end time of one maintenance event
    start <- log$TIMESTAMP_start[i]
    end <- log$TIMESTAMP_end[i]


    # get indices of columns affected by maintenance
    if(grepl("^\\d+$", log$colnumber[i])) # single num
    {
      maintenance_cols <- intersect(c(2:41), as.integer(log$colnumber[i]))
    }

    else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$colnumber[i])) # c(x;y;...)
    {
      maintenance_cols <- intersect(c(2:41), as.integer(unlist(regmatches(log$colnumber[i],
                                                                          gregexpr("\\d+", log$colnumber[i])))))
    }
    else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$colnumber[i])) # c(x:y)
    {
      bounds <- as.integer(unlist(regmatches(log$colnumber[i], gregexpr("\\d+", log$colnumber[i]))))
      maintenance_cols <- intersect(c(2:41), c(bounds[1]:bounds[2]))
    }
    else
    {
      warning(paste("Could not parse column colnumber in row", i, "of the maintenance log. Skipping maintenance for",
                    "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                    "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                    "Other values (even valid calls to c()) will not be parsed properly."))
      next
    }

    # remove EXO_Date and EXO_Time columns from the list of maintenance columns, because they will be deleted later
    maintenance_cols <- setdiff(maintenance_cols, c(21, 22))

    if(length(maintenance_cols) == 0)
    {
      warning(paste("Did not parse any valid data columns in row", i, "of the maintenance log. Valid columns have",
                    "indices 2 through 41, excluding 21 and 22, which are deleted by this script. Skipping maintenance for that row."))
      next
    }

    #index the Flag columns
    if(grepl("^\\d+$", log$flagcol[i])) # single num
    {
      flag_cols <- intersect(c(42:80), as.integer(log$flagcol[i]))

    }
    else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$flagcol[i])) # c(x;y;...)
    {
      flag_cols <- intersect(c(42:80), as.integer(unlist(regmatches(log$flagcol[i],
                                                                    gregexpr("\\d+", log$flagcol[i])))))
    }

    else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$flagcol[i])) # c(x:y)
    {
      bounds_flag <- as.integer(unlist(regmatches(log$flagcol[i], gregexpr("\\d+", log$flagcol[i]))))
      flag_cols <- intersect(c(42:80), c(bounds_flag[1]:bounds_flag[2]))
    }
    else
    {
      warning(paste("Could not parse column flagcol in row", i, "of the maintenance log. Skipping maintenance for",
                    "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                    "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                    "Other values (even valid calls to c()) will not be parsed properly."))
      next
    }

    #Get the Maintenance Flag

    flag <- log$flag[i]

    # replace relevant data with NAs and set flags while maintenance was in effect
    if(flag==5)
    {
      print(paste("Row",i,"are questionable values but kept in data frame"))
    }
    else if(flag==8 && maintenance_cols==6)
    {
      catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] <- catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols]-0.22617
    }
    else if (flag==8 && maintenance_cols==9)
    {
      catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] <- catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols]-0.18122
    }
    else if (flag==6 && maintenance_cols==15) #adjusting the RDO_5_mgL
    {
      dt=catdata[catdata$DateTime >= start & catdata$DateTime <= end, 1]

      catdata[catdata$DateTime >= start & catdata$DateTime <= end, 42] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
        sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/30
    }
    else if (flag==6 && maintenance_cols==16) #adjusting the RDO_5_sat
    {
      dt=catdata[catdata$DateTime >= start & catdata$DateTime <= end, 1]

      catdata[catdata$DateTime >= start & catdata$DateTime <= end, 43] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
        sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/30/11.3*100
    }
    else if (flag==6 && maintenance_cols==18) #adjusting the RDO_9_mgl
    {
      dt=catdata[catdata$DateTime >= start & catdata$DateTime <= end, 1]

      if(start=="2020-08-19 16:00:00" || start=="2020-08-11 03:00:00") #figure out how to get this to work
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end,44] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/6500
        print(paste("Row",i,"is in the maintenance log for 2020-08-19 20:00:00 or 2020-08-11 07:00:00"))
      }
      else if (start=="2020-08-26 08:00:00")
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end, 44] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/10000
        print(paste("Row",i,"is in the maintenance log for 2020-08-26 12:00:00"))
      }
      else if (start=="2020-09-05 02:00:00")
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end, 44] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/3000
        print(paste("Row",i,"is in the maintenance log for 2020-09-05 06:00:00"))
      }
      else{
        warning(paste("Make sure row",i,"is in the maintenance log"))
      }
      #catdata[catdata$DateTime >= start & catdata$DateTime <= end, flag_cols] <- flag
    }
    else if (flag==6 && maintenance_cols==19) #adjusting the RDO_9_sat
    {
      dt=catdata[catdata$DateTime >= start & catdata$DateTime <= end, 1]

      if(start=="2020-08-19 16:00:00" || start=="2020-08-11 03:00:00")
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end, 45] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/6500/11.3*100
        print(paste("Row",i,"is in the maintenance log for 2020-08-19 20:00:00 or 2020-08-11 07:00:00"))
      }
      else if (start=="2020-08-26 08:00:00")
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end, 45] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/10000/11.3*100
        print(paste("Row",i,"is in the maintenance log for 2020-08-26 12:00:00"))
      }
      else if (start=="2020-09-05 02:00:00")
      {
        catdata[catdata$DateTime >= start & catdata$DateTime <= end, 45] = catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] +
          sqrt(as.numeric(difftime(dt$DateTime, start, units = "mins")))/3000/11.3*100
        print(paste("Row",i,"is in the maintenance log for 2020-09-05 06:00:00"))
      }
      else{
        warning(paste("Make sure row",i,"is in the maintenance log"))
      }
    }
    else
    {
      catdata[catdata$DateTime >= start & catdata$DateTime <= end, maintenance_cols] <- NA
    }
    #Add the 2 hour adjustment for DO
    if (log$colnumber[i]=="c(1:41)" && flag==1){
      DO_col=c("RDO_mgL_5", "RDOsat_percent_5", "RDO_mgL_9","RDOsat_percent_9","EXODOsat_percent_1", "EXODO_mgL_1")
      catdata[catdata$DateTime>start&catdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
    }
    else if(log$colnumber[i] %in% c("15","16") && flag==1){
      DO_col=c("RDO_mgL_5", "RDOsat_percent_5")
      catdata[catdata$DateTime>start&catdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
    }
    else if(log$colnumber[i] %in% c("18","19") && flag==1){
      DO_col=c("RDO_mgL_9","RDOsat_percent_9")
      catdata[catdata$DateTime>start&catdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
    }
    else if (log$colnumber[i] %in% c("c(21:39","27","28") && flag==1){
      DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1")
      catdata[catdata$DateTime>start&catdata$DateTime<end+ADJ_PERIOD,DO_col] <- NA
    }
    else{
      warning(paste("No DO time to adjust in row",i,"."))
    }
  }

  ########################################################################################################################
  # Fill in adjusted DO values that didn't get changed. If the value didn't get changed then it is the same as values from the
  # non adjusted columns

  catdata=catdata%>%
    dplyr::mutate(
      RDO_mgL_5_adjusted=ifelse(RDO_mgL_5_adjusted==0, RDO_mgL_5, RDO_mgL_5_adjusted),
      RDOsat_percent_5_adjusted=ifelse(RDOsat_percent_5_adjusted==0, RDOsat_percent_5, RDOsat_percent_5_adjusted),
      RDO_mgL_9_adjusted=ifelse(RDO_mgL_9_adjusted==0, RDO_mgL_9, RDO_mgL_9_adjusted),
      RDOsat_percent_9_adjusted=ifelse(RDOsat_percent_9_adjusted==0, RDOsat_percent_9, RDOsat_percent_9_adjusted)
    )

  ########################################################################################################################

  # find chla and phyo on the EXO sonde sensor data that differs from the mean by more than the standard deviation times a given factor, and
  # replace with NAs between October 2018 and March 2019, due to sensor fouling
  Chla_RFU_1_mean <- mean(catdata$EXOChla_RFU_1, na.rm = TRUE)
  Chla_ugL_1_mean <- mean(catdata$EXOChla_ugL_1, na.rm = TRUE)
  BGAPC_RFU_1_mean <- mean(catdata$EXOBGAPC_RFU_1, na.rm = TRUE)
  BGAPC_ugL_1_mean <- mean(catdata$EXOBGAPC_ugL_1, na.rm = TRUE)
  Chla_RFU_1_threshold <- EXO_FOULING_FACTOR * sd(catdata$EXOChla_RFU_1, na.rm = TRUE)
  Chla_ugL_1_threshold <- EXO_FOULING_FACTOR * sd(catdata$EXOChla_ugL_1, na.rm = TRUE)
  BGAPC_RFU_1_threshold <- EXO_FOULING_FACTOR * sd(catdata$EXOBGAPC_RFU_1, na.rm = TRUE)
  BGAPC_ugL_1_threshold <- EXO_FOULING_FACTOR * sd(catdata$EXOBGAPC_ugL_1, na.rm = TRUE)


  # QAQC on major chl outliers using DWH's method: datapoint set to NA if data is greater than 4*sd different from both previous and following datapoint
  catdata <- catdata %>%
    dplyr::mutate(Chla_ugL = lag(EXOChla_ugL_1, 0),
                  Chla_ugL_lag1 = lag(EXOChla_ugL_1, 1),
                  Chla_ugL_lead1 = lead(EXOChla_ugL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOChla_ugL_1 = ifelse((abs(Chla_ugL_lag1 - Chla_ugL) > (Chla_ugL_1_threshold))  & (abs(Chla_ugL_lead1 - Chla_ugL) > (Chla_ugL_1_threshold) & !is.na(Chla_ugL)),
                                         NA, EXOChla_ugL_1)) %>%
    dplyr::select(-Chla_ugL, -Chla_ugL_lag1, -Chla_ugL_lead1)

  #Chla_RFU QAQC
  catdata <- catdata %>%
    dplyr::mutate(Chla_RFU = lag(EXOChla_RFU_1, 0),
                  Chla_RFU_lag1 = lag(EXOChla_RFU_1, 1),
                  Chla_RFU_lead1 = lead(EXOChla_RFU_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOChla_RFU_1 = ifelse((abs(Chla_RFU_lag1 - Chla_RFU) > (Chla_RFU_1_threshold))  & (abs(Chla_RFU_lead1 - Chla_RFU) > (Chla_RFU_1_threshold) & !is.na(Chla_RFU)),
                                         NA, EXOChla_RFU_1)) %>%
    dplyr::select(-Chla_RFU, -Chla_RFU_lag1, -Chla_RFU_lead1)

  # QAQC on major chl outliers using DWH's method: datapoint set to NA if data is greater than 4*sd different from both previous and following datapoint
  catdata <- catdata %>%
    dplyr::mutate(phyco_ugL = lag(EXOBGAPC_ugL_1, 0),
                  phyco_ugL_lag1 = lag(EXOBGAPC_ugL_1, 1),
                  phyco_ugL_lead1 = lead(EXOBGAPC_ugL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOBGAPC_ugL_1 = ifelse((abs(phyco_ugL_lag1 - phyco_ugL) > (BGAPC_ugL_1_threshold))  & (abs(phyco_ugL_lead1 - phyco_ugL) > (BGAPC_ugL_1_threshold) & !is.na(phyco_ugL)),
                                          NA, EXOBGAPC_ugL_1)) %>%
    dplyr::select(-phyco_ugL, -phyco_ugL_lag1, -phyco_ugL_lead1)

  #Phyco QAQC for RFU
  catdata <- catdata %>%
    dplyr::mutate(phyco_RFU = lag(EXOBGAPC_RFU_1, 0),
                  phyco_RFU_lag1 = lag(EXOBGAPC_RFU_1, 1),
                  phyco_RFU_lead1 = lead(EXOBGAPC_RFU_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOBGAPC_RFU_1 = ifelse((abs(phyco_RFU_lag1 - phyco_RFU) > (BGAPC_RFU_1_threshold))  & (abs(phyco_RFU_lead1 - phyco_RFU) > (BGAPC_RFU_1_threshold) & !is.na(phyco_RFU)),
                                          NA, EXOBGAPC_RFU_1)) %>%
    dplyr::select(-phyco_RFU, -phyco_RFU_lag1, -phyco_RFU_lead1)

  #QAQC major outliers during the winter of 2018 going into 2019 due to fouling that did not get caught above. These points are removed
  catdata <- catdata %>%
    dplyr::mutate(EXOChla_RFU_1 = ifelse(DateTime >= lubridate::ymd("2018-10-01") & DateTime < lubridate::ymd("2019-03-01") &
                                           abs(EXOChla_RFU_1 - Chla_RFU_1_mean) > Chla_RFU_1_threshold, NA, EXOChla_RFU_1)) %>%
    dplyr::mutate(EXOChla_ugL_1 = ifelse(DateTime >= lubridate::ymd("2018-10-01") & DateTime < lubridate::ymd("2019-03-01") &
                                           abs(EXOChla_ugL_1 - Chla_ugL_1_mean) > Chla_ugL_1_threshold, NA, EXOChla_ugL_1)) %>%
    dplyr::mutate(EXOBGAPC_RFU_1 = ifelse(DateTime >= lubridate::ymd("2018-10-01") & DateTime < lubridate::ymd("2019-03-01") &
                                            abs(EXOBGAPC_RFU_1 - BGAPC_RFU_1_mean) > BGAPC_RFU_1_threshold, NA, EXOBGAPC_RFU_1)) %>%
    dplyr::mutate(EXOBGAPC_ugL_1 = ifelse(DateTime >= lubridate::ymd("2018-10-01") & DateTime < lubridate::ymd("2019-03-01") &
                                            abs(EXOBGAPC_ugL_1 - BGAPC_ugL_1_mean) > BGAPC_ugL_1_threshold, NA, EXOBGAPC_ugL_1))


  ####################################################################################################################################
  # fdom qaqc----
  # QAQC from DWH to remove major outliers from fDOM data that are 2 sd's greater than the previous and following datapoint
  sd_fDOM_QSU <- sd(catdata$EXOfDOM_QSU_1, na.rm = TRUE) #deteriming the standard deviation of fDOM data
  sd_fDOM_RFU <- sd(catdata$EXOfDOM_RFU_1, na.rm = TRUE)
  mean_fDOM_QSU <- mean(catdata$EXOfDOM_QSU_1, na.rm = TRUE) #deteriming the standard deviation of fDOM data
  mean_fDOM_RFU <- mean(catdata$EXOfDOM_RFU_1, na.rm = TRUE)

  #fDOM QSU QAQC
  catdata <- catdata%>%
    #mutate(Flag_fDOM = ifelse(is.na(EXOfDOM_QSU_1), 1, 0)) #This creates Flag column for fDOM data, setting all NA's going into QAQC as 1 for missing data, and to 0 for the rest
    dplyr::mutate(fDOM_QSU = lag(EXOfDOM_QSU_1, 0),
                  fDOM_QSU_lag1 = lag(EXOfDOM_QSU_1, 1),
                  fDOM_QSU_lead1 = lead(EXOfDOM_QSU_1, 1)) %>%  #These mutates create columns for current fDOM_QSU, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOfDOM_QSU_1 = ifelse(fDOM_QSU < 0 & !is.na(fDOM_QSU), NA, EXOfDOM_QSU_1))%>%
    dplyr::mutate(EXOfDOM_QSU_1 = ifelse(
      ( abs(fDOM_QSU_lag1 - fDOM_QSU) > (2*sd_fDOM_QSU)   )  & ( abs(fDOM_QSU_lead1 - fDOM_QSU) > (2*sd_fDOM_QSU)  & !is.na(fDOM_QSU) ), NA, EXOfDOM_QSU_1
    )) %>%  #QAQC to remove outliers for QSU fDOM data
    dplyr::select(-fDOM_QSU, -fDOM_QSU_lag1, -fDOM_QSU_lead1)#This removes the columns used to run ifelse statements since they are no longer needed.


  #fDOM QSU QAQC
  catdata <- catdata%>%
    #mutate(Flag_fDOM = ifelse(is.na(EXOfDOM_RFU_1), 1, 0)) #This creates Flag column for fDOM data, setting all NA's going into QAQC as 1 for missing data, and to 0 for the rest
    dplyr::mutate(fDOM_RFU = lag(EXOfDOM_RFU_1, 0),
                  fDOM_RFU_lag1 = lag(EXOfDOM_RFU_1, 1),
                  fDOM_RFU_lead1 = lead(EXOfDOM_RFU_1, 1)) %>%  #These mutates create columns for current fDOM_RFU, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOfDOM_RFU_1 = ifelse(fDOM_RFU < 0 & !is.na(fDOM_RFU), NA, EXOfDOM_RFU_1))%>%
    dplyr::mutate(EXOfDOM_RFU_1 = ifelse(
      ( abs(fDOM_RFU_lag1 - fDOM_RFU) > (2*sd_fDOM_RFU)   )  & ( abs(fDOM_RFU_lead1 - fDOM_RFU) > (2*sd_fDOM_RFU)  & !is.na(fDOM_RFU) ), NA, EXOfDOM_RFU_1
    )) %>%
    dplyr::select(-fDOM_RFU, -fDOM_RFU_lag1, -fDOM_RFU_lead1)#This removes the columns used to run ifelse statements since they are no longer needed.

  #####################################################################################################################################
  #QAQC from DWH to remove major outliers from conductity, specific conductivity and TDS data that are 2 sd's greater than the previous and following datapoint


  sd_cond <- sd(catdata$EXOCond_uScm_1, na.rm = TRUE)
  sd_spcond <-sd(catdata$EXOSpCond_uScm_1, na.rm = TRUE)
  sd_TDS <- sd(catdata$EXOTDS_mgL_1, na.rm = TRUE)
  mean_cond <- mean(catdata$EXOCond_uScm_1, na.rm = TRUE)
  mean_spcond <-mean(catdata$EXOSpCond_uScm_1, na.rm = TRUE)
  mean_TDS <- mean(catdata$EXOTDS_mgL_1, na.rm = TRUE)


  # QAQC on major conductivity outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
  #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
  catdata <- catdata %>%
    dplyr::mutate(Cond = lag(EXOCond_uScm_1, 0),
                  Cond_lag1 = lag(EXOCond_uScm_1, 1),
                  Cond_lead1 = lead(EXOCond_uScm_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOCond_uScm_1 = ifelse(Cond < 1 & !is.na(Cond), NA, EXOCond_uScm_1)) %>%
    dplyr::mutate(EXOCond_uScm_1 = ifelse((abs(Cond_lag1 - Cond) > (2*sd_cond))  & (abs(Cond_lead1 - Cond) > (2*sd_cond) & !is.na(Cond)),
                                          NA, EXOCond_uScm_1)) %>%
    dplyr::select(-Cond, -Cond_lag1, -Cond_lead1)

  # QAQC on major Specific conductivity outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
  #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
  catdata <- catdata %>%
    dplyr::mutate(SpCond = lag(EXOSpCond_uScm_1, 0),
                  SpCond_lag1 = lag(EXOSpCond_uScm_1, 1),
                  SpCond_lead1 = lead(EXOSpCond_uScm_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOSpCond_uScm_1 = ifelse((abs(SpCond_lag1 - SpCond) > (2*sd_spcond))  & (abs(SpCond_lead1 - SpCond) > (2*sd_spcond) & !is.na(SpCond)),
                                            NA, EXOSpCond_uScm_1)) %>%
    dplyr::select(-SpCond, -SpCond_lag1, -SpCond_lead1)

  # QAQC on major TDS outliers using DWH's method: datapoint set to NA if data is greater than 2*sd different from both previous and following datapoint
  #Remove any data below 1 because it is an outlier and give it a Flag Code of 2
  catdata <- catdata %>%
    dplyr::mutate(TDS = lag(EXOTDS_mgL_1, 0),
                  TDS_lag1 = lag(EXOTDS_mgL_1, 1),
                  TDS_lead1 = lead(EXOTDS_mgL_1, 1)) %>%  #These mutates create columns for current fDOM, fDOM before and fDOM after. These are used to run ifelse QAQC loops
    dplyr::mutate(EXOTDS_mgL_1 = ifelse(TDS < 1 & !is.na(TDS), NA, EXOTDS_mgL_1)) %>%
    dplyr::mutate(EXOTDS_mgL_1 = ifelse((abs(TDS_lag1 - TDS) > (2*sd_TDS))  & (abs(TDS_lead1 - TDS) > (2*sd_TDS) & !is.na(TDS)),
                                        NA, EXOTDS_mgL_1)) %>%
    dplyr::select(-TDS, -TDS_lag1, -TDS_lead1)

  #####################################################################################################################################
  #change EXO values to NA if EXO depth is less than 0.5m

  #index only the colummns with EXO at the beginning
  exo_idx <-grep("^EXO",colnames(catdata))

  #Change the EXO data to NAs when the EXO is above 0.5m and not due to maintenance
  catdata[which(catdata$EXO_depth_m < 0.55), exo_idx] <- NA
  ########################################################################################################################
  # delete EXO_Date and EXO_Time columns
  catdata <- catdata %>% dplyr::select(-EXO_Date, -EXO_Time)

  #create depth column
  # convert psi into meters from the pressure sensor at 9m

  catdata=catdata%>%dplyr::mutate(Depth_m_9=Lvl_psi_9*0.70455)#1psi=2.31ft, 1ft=0.305m


  # reorder columns
  catdata <- catdata %>% select(DateTime, ThermistorTemp_C_surface:ThermistorTemp_C_9,
                                RDO_mgL_5,RDO_mgL_5_adjusted, RDOsat_percent_5,RDOsat_percent_5_adjusted,
                                RDOTemp_C_5, RDO_mgL_9,RDO_mgL_9_adjusted, RDOsat_percent_9,RDOsat_percent_9_adjusted, RDOTemp_C_9,
                                EXOTemp_C_1, EXOCond_uScm_1, EXOSpCond_uScm_1, EXOTDS_mgL_1, EXODOsat_percent_1,
                                EXODO_mgL_1, EXOChla_RFU_1, EXOChla_ugL_1, EXOBGAPC_RFU_1, EXOBGAPC_ugL_1,
                                EXOfDOM_RFU_1, EXOfDOM_QSU_1, EXO_pressure_psi, EXO_depth_m, EXO_battery_V, EXO_cablepower_V,
                                EXO_wiper_V, Lvl_psi_9, Depth_m_9,LvlTemp_C_9, RECORD, CR6_Batt_V, CR6Panel_Temp_C)
  # replace NaNs with NAs
  catdata[is.na(catdata)] <- NA

  ###############################################################################################################

  if(!is.na(qaqc_file)){
    #Different lakes are going to have to modify this for their temperature data format

    d1 <- catdata

    d2 <- read.csv(qaqc_file, na.strings = 'NA', stringsAsFactors = FALSE)
    TIMESTAMP_in <- as_datetime(d1$DateTime,tz = input_file_tz)
    d1$TIMESTAMP <- with_tz(TIMESTAMP_in,tz = "UTC")

    TIMESTAMP_in <- as_datetime(d2$DateTime,tz = input_file_tz)
    d2$TIMESTAMP <- with_tz(TIMESTAMP_in,tz = "UTC")

    # add a depth column from the pressure sensor because it is not in the EDI file
    d2$Depth_m_9=d2$Lvl_psi_9*0.70455 # 1psi=2.31ft. 1ft=0.305m

    d1 <- d1[which(d1$TIMESTAMP > d2$TIMESTAMP[nrow(d2)] | d1$TIMESTAMP < d2$TIMESTAMP[1]), ]

    #d3 <- data.frame(TIMESTAMP = d1$TIMESTAMP, wtr_surface = d1$wtr_surface, wtr_1 = d1$wtr_1, wtr_2 = d1$wtr_2, wtr_3 = d1$wtr_3, wtr_4 = d1$wtr_4,
    #                 wtr_5 = d1$wtr_5, wtr_6 = d1$wtr_6, wtr_7 = d1$wtr_7, wtr_8 = d1$wtr_8, wtr_9 = d1$wtr_9, wtr_1_exo = d1$EXO_wtr_1, wtr_5_do = d1$dotemp_5, wtr_9_do = d1$dotemp_9)

    #changed the name so then we can separate into multiple columns for later on
    d3 <-  data.frame(TIMESTAMP = d1$TIMESTAMP, wtr_0.0_ther = d1$ThermistorTemp_C_surface,
                      wtr_1.0_ther = d1$ThermistorTemp_C_1, wtr_2.0_ther = d1$ThermistorTemp_C_2,
                      wtr_3.0_ther = d1$ThermistorTemp_C_3, wtr_4.0_ther = d1$ThermistorTemp_C_4,
                      wtr_5.0_ther = d1$ThermistorTemp_C_5, wtr_6.0_ther = d1$ThermistorTemp_C_6,
                      wtr_7.0_ther = d1$ThermistorTemp_C_7, wtr_8.0_ther = d1$ThermistorTemp_C_8,
                      wtr_9.0_ther = d1$ThermistorTemp_C_9, wtr_1.6_exo = d1$EXOTemp_C_1,
                      wtr_5.0_do = d1$RDOTemp_C_5, wtr_9.0_do = d1$RDOTemp_C_9,
                      chla_1.6_exo = d1$EXOChla_ugL_1, doobs_1.6_exo = d1$EXODO_mgL_1,
                      doobs_5.0_do = d1$RDO_mgL_5_adjusted, doobs_9.0_do = d1$RDO_mgL_9_adjusted,
                      fdom_1.6_exo = d1$EXOfDOM_QSU_1, bgapc_1.6_exo = d1$EXOBGAPC_ugL_1,
                      depth_1.6_exo = d1$EXO_depth_m, depth_9.0_psi=d1$Depth_m_9)

    #changed the name so then we can separate into multiple columns for later on
    d4 <- data.frame(TIMESTAMP = d2$TIMESTAMP, wtr_0.0_ther = d2$ThermistorTemp_C_surface,
                     wtr_1.0_ther = d2$ThermistorTemp_C_1, wtr_2.0_ther = d2$ThermistorTemp_C_2,
                     wtr_3.0_ther = d2$ThermistorTemp_C_3, wtr_4.0_ther = d2$ThermistorTemp_C_4,
                     wtr_5.0_ther = d2$ThermistorTemp_C_5, wtr_6.0_ther = d2$ThermistorTemp_C_6,
                     wtr_7.0_ther = d2$ThermistorTemp_C_7, wtr_8.0_ther = d2$ThermistorTemp_C_8,
                     wtr_9.0_ther = d2$ThermistorTemp_C_9, wtr_1.6_exo = d2$EXOTemp_C_1,
                     wtr_5.0_do = d2$RDOTemp_C_5, wtr_9.0_do = d2$RDOTemp_C_9,
                     chla_1.6_exo = d2$EXOChla_ugL_1, doobs_1.6_exo = d2$EXODO_mgL_1,
                     doobs_5.0_do = d2$RDO_mgL_5_adjusted, doobs_9.0_do = d2$RDO_mgL_9_adjusted,
                     fdom_1.6_exo = d2$EXOfDOM_QSU_1, bgapc_1.6_exo = d2$EXOBGAPC_ugL_1,
                     depth_1.6_exo = d2$EXO_depth_m, depth_9.0_psi=d2$Depth_m_9)

    d <- rbind(d3,d4)

    #take out EXO depth and pressure sensor depth for later
    pres=d%>%
      select(TIMESTAMP,depth_1.6_exo,depth_9.0_psi)

  }else{
    #Different lakes are going to have to modify this for their temperature data format
    d1 <- catdata

    TIMESTAMP_in <- as_datetime(d1$DateTime,tz = input_file_tz)
    d1$TIMESTAMP <- with_tz(TIMESTAMP_in,tz = "UTC")

    #changed the name so then we can separate into multiple columns for later on
    d <-  data.frame(TIMESTAMP = d1$TIMESTAMP, wtr_0.0_ther = d1$ThermistorTemp_C_surface,
                     wtr_1.0_ther = d1$ThermistorTemp_C_1, wtr_2.0_ther = d1$ThermistorTemp_C_2,
                     wtr_3.0_ther = d1$ThermistorTemp_C_3, wtr_4.0_ther = d1$ThermistorTemp_C_4,
                     wtr_5.0_ther = d1$ThermistorTemp_C_5, wtr_6.0_ther = d1$ThermistorTemp_C_6,
                     wtr_7.0_ther = d1$ThermistorTemp_C_7, wtr_8.0_ther = d1$ThermistorTemp_C_8,
                     wtr_9.0_ther = d1$ThermistorTemp_C_9, wtr_1.6_exo = d1$EXOTemp_C_1,
                     wtr_5.0_do = d1$RDOTemp_C_5, wtr_9.0_do = d1$RDOTemp_C_9,
                     chla_1.6_exo = d1$EXOChla_ugL_1, doobs_1.6_exo = d1$EXODO_mgL_1,
                     doobs_5.0_do = d1$RDO_mgL_5_adjusted, doobs_9.0_do = d1$RDO_mgL_9_adjusted,
                     fdom_1.6_exo = d1$EXOfDOM_QSU_1, bgapc_1.6_exo = d1$EXOBGAPC_ugL_1,
                     depth_1.6_exo = d1$EXO_depth_m, depth_9.0_psi=d1$Depth_m_9)

    #take out EXO depth and pressure sensor depth for later
    pres=d%>%
      select(TIMESTAMP,depth_1.6_exo,depth_9.0_psi)
  }


  d$fDOM_1 <- config$exo_sensor_2_grab_sample_fdom[1] + config$exo_sensor_2_grab_sample_fdom[2] * d$fDOM_1

  #oxygen unit conversion
  d$doobs_1 <- d$doobs_1*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)
  d$doobs_5 <- d$doobs_5_adjusted*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)
  d$doobs_9 <- d$doobs_9_adjusted*1000/32  #mg/L (obs units) -> mmol/m3 (glm units)

  d$chla_1 <-  config$exo_sensor_2_ctd_chla[1] +  d$chla_1 *  config$exo_sensor_2_ctd_chla[2]
  d$doobs_1 <- config$exo_sensor_2_ctd_do[1]  +   d$doobs_1 * config$exo_sensor_2_ctd_do[2]
  d$doobs_5 <- config$do_sensor_2_ctd_do_5[1] +   d$doobs_5 * config$do_sensor_2_ctd_do_5[2]
  d$doobs_9 <- config$do_sensor_2_ctd_do_9[1] +   d$doobs_9 * config$do_sensor_2_ctd_do_9[2]

  d <- d %>%
    dplyr::mutate(day = day(TIMESTAMP),
                  year = year(TIMESTAMP),
                  hour = hour(TIMESTAMP),
                  month = month(TIMESTAMP)) %>%
    dplyr::group_by(day, year, hour, month) %>%
    dplyr::select(-TIMESTAMP) %>%
    dplyr::summarise_all(mean, na.rm = TRUE) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(day = as.numeric(day),
                  hour = as.numeric(hour)) %>%
    dplyr::mutate(day = ifelse(as.numeric(day) < 10, paste0("0",day),day),
                  hour = ifelse(as.numeric(hour) < 10, paste0("0",hour),hour)) %>%
    dplyr::mutate(timestamp = as_datetime(paste0(year,"-",month,"-",day," ",hour,":00:00"),tz = "UTC")) %>%
    dplyr::arrange(timestamp)



  # Makes the data file long into three columns with TIMESTAMP, Name of sensor with depth and the reading from the sensor
  d_long=d%>%
    tidyr::pivot_longer(cols=-TIMESTAMP, names_to="Name", values_to="value")

  # Separate the Name column into variable, depth and method. tidyr::seperate takes too long use this function
  d_long[c('variable', 'depth', 'method')] <- str_split_fixed(d_long$Name, '_', 3)

  # Rename the variable and methods. Take out depth variable and select needed columns
  d_long=d_long%>%
    dplyr::mutate(variable=ifelse(variable=="wtr", "temperature",variable),
                  variable=ifelse(variable== "doobs", "oxygen", variable),
                  variable=ifelse(variable== "depth", "depth", variable),
                  method=ifelse(method== "ther", "thermistor", method),
                  method=ifelse(method== "exo", "exo_sensor", method),
                  method=ifelse(method== "do", "do_sensor", method),
                  method=ifelse(method== "psi", "pressure_sensor", method))%>%
    dplyr::filter(variable!="depth")%>%
    dplyr::select(TIMESTAMP, method, variable, depth, value)%>%
    dplyr::mutate(depth=as.numeric(depth))




  if(config$variable_obsevation_depths == TRUE){

    # this is how you would set up code if water level changes. Because the depth of the EXO changed from 1m to 1.6m
    # and the pressure sensor was added in May 2020 I decided to write the code as if we had the pressures sensor
    # the whole time.

    d_depth=merge(d_long,pres, by="TIMESTAMP") #combine the depth values from the EXO and the pressure sensor

    #offset for EXO

    #First need to create a data frame of the offsets
    depth <- c(0.0,1.0,1.6,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0)
    #exo_offset <-c(-1.5,-0.6,0,0.4,1.4,2.4,3.4,4.4,5.4,6.4,7.4) #the offset if you just had the EXO which is at 1.6m
    pres_offset <-c(9.0,8.0,NA,7.0,6.0,5.0,4.0,3.0,2.0,1.0,0.0) #offset from pressure transducer
    offset<-data.frame(depth,pres_offset)

    d_depth=merge(d_depth, offset, by='depth', all=T)

    # Get the actual depth of the sensors based on water pressure

    d_long=d_depth%>%
      dplyr::mutate(depth=ifelse(method!="exo_sensor", depth_9.0_psi-pres_offset, depth), #depth based on pressure sensor
                    depth=ifelse(method=="exo_sensor", depth_1.6_exo, depth), #depth of EXO
                    depth = round(depth, 2))%>%
      dplyr::filter(depth > 0.0) #take out NAs and when the sensors are out of the water
    dplyr::mutate(depth=round(depth,2))%>% #round depth to the hundredths place
      dplyr::select(TIMESTAMP, method, variable, depth, value)

  }


  d <- d_long

  d <- d %>% mutate(depth = as.numeric(depth))



  # write to output file
  return(d)
}

