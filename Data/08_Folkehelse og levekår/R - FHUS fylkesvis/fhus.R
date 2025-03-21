library(tidyverse)

theme_set(theme_bw()) #set ggplot2 theme
setwd("C:/Users/eve1509/OneDrive - Vestfold og Telemark fylkeskommune/Github/Telemark/Data/08_Folkehelse og levekår/R - FHUS fylkesvis/")
getwd()

kommune_raw <- read_csv("input/V&T.csv") #Datasett med kommuner
#land <- read_csv("input/V&Tlandgr.csv") #Info om landbakgrunn, men ikke kommune

# Ctrl + Shift + M = %>%

# "Group_by" tilsvarer de gruppene (og undergruppene) man ønsker på x-aksen. Rekkefølge betyr ikke nødvendigvis noe.

# -------------------------- Tilføye innvandrerstatus til tabell med landbakgrunn ----------------------- #

#land <- land %>% 
#  mutate(Innvandrerstatus = case_when(QFVT_7_8D != "JEG HAR BODD I NORGE HELE LIVET" ~ "Innvandrere",
#                                      QFVT_7_8 == "NEI" | (QFVT_7_8 == "JA" & QFVT_7_8D == "JEG HAR BODD I NORGE HELE LIVET") ~ "Ikke-innvandrere"))

#unique(land$Innvandrerstatus)
#land %>% count(Innvandrerstatus)

#Innvandrere: Er selv, eller har forelder(e) som er født i utlandet, OG har selv ikke bodd i Norge hele livet.

#Ikke-innvandrere: Verken selv eller forelder født i utlandet, og har bodd i Norge hele livet.

## Sette opp datasett

kommune <- kommune_raw %>% rename(alder = alderkat,
                            kjønn = Kjonn_Kode,
                            utdanning = utdannelse,
                            trivsel_nærmiljø = QFVT_1_1,
                            trygg_nærmiljø = QFVT_6_17,
                            org_aktivitet = QFVT_1_9,
                            annen_aktivitet = QFVT_1_10,
                            egen_helse = QFVT_2_1,
                            egen_tannhelse = QFVT_2_2,
                            tannlegebesøk = QFVT_2_3,
                            kost_bær = QFVT_4_5,
                            kost_grønt = QFVT_4_6,
                            skader = QFVT_5_1,
                            fornøyd_livet = QFVT_6_1) %>%
  mutate(
    org_aktivitet_kat = case_when(
      org_aktivitet == "DAGLIG" ~ "MINST ÉN GANG I UKA",
      org_aktivitet == "UKENTLIG" ~ "MINST ÉN GANG I UKA",
      org_aktivitet == "1-3 GANGER PER MÅNED" ~ "MINDRE ENN ÉN GANG I UKA",
      org_aktivitet == "SJELDNERE" ~ "MINDRE ENN ÉN GANG I UKA",
      org_aktivitet == "ALDRI" ~ "MINDRE ENN ÉN GANG I UKA"
    )
  ) %>%
  mutate(
    annen_aktivitet_kat = case_when(
      annen_aktivitet == "DAGLIG" ~ "MINST ÉN GANG I UKA",
      annen_aktivitet == "UKENTLIG" ~ "MINST ÉN GANG I UKA",
      annen_aktivitet == "1-3 GANGER PER MÅNED" ~ "MINDRE ENN ÉN GANG I UKA",
      annen_aktivitet == "SJELDNERE" ~ "MINDRE ENN ÉN GANG I UKA",
      annen_aktivitet == "ALDRI" ~ "MINDRE ENN ÉN GANG I UKA"
    )
  ) %>%
  mutate(
    egen_helse_kat = case_when(
      egen_helse == "SVÆRT GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_helse == "GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_helse == "VERKEN GOD ELLER DÅRLIG" ~ "MINDRE ENN GOD ELLER SVÆRT GOD",
      egen_helse == "DÅRLIG" ~ "MINDRE ENN GOD ELLER SVÆRT GOD",
      egen_helse == "SVÆRT DÅRLIG" ~ "MINDRE ENN GOD ELLER SVÆRT GOD"
    )
  ) %>%
  mutate(
    kost_bær_kat = case_when(
      kost_bær == "SJELDEN/ALDRI" ~ "Sjeldnere enn daglig",
      kost_bær == "1-3 GANGER PER MÅNED" ~ "Sjeldnere enn daglig",
      kost_bær == "1 GANG PER UKE" ~ "Sjeldnere enn daglig",
      kost_bær == "2-3 GANGER PER UKE" ~ "Sjeldnere enn daglig",
      kost_bær == "4-6 GANGER PER UKE" ~ "Sjeldnere enn daglig",
      kost_bær == "1 GANG PER DAG" ~ "Daglig",
      kost_bær == "FLERE GANGER PER DAG" ~ "Daglig"
    )
  ) %>%
  mutate(
    kost_grønt_kat = case_when(
      kost_grønt == "SJELDEN/ALDRI" ~ "Sjeldnere enn daglig",
      kost_grønt == "1-3 GANGER PER MÅNED" ~ "Sjeldnere enn daglig",
      kost_grønt == "1 GANG PER UKE" ~ "Sjeldnere enn daglig",
      kost_grønt == "2-3 GANGER PER UKE" ~ "Sjeldnere enn daglig",
      kost_grønt == "4-6 GANGER PER UKE" ~ "Sjeldnere enn daglig",
      kost_grønt == "1 GANG PER DAG" ~ "Daglig",
      kost_grønt == "FLERE GANGER PER DAG" ~ "Daglig"
    )
  ) %>%
  mutate(
    fornøyd_livet_kat = case_when(
      fornøyd_livet == "0 - IKKE FORNØYD I DET HELE TATT" ~ "0",
      fornøyd_livet == "1" ~ "1",
      fornøyd_livet == "2" ~ "2",
      fornøyd_livet == "3" ~ "3",
      fornøyd_livet == "4" ~ "4",
      fornøyd_livet == "5" ~ "5",
      fornøyd_livet == "6" ~ "6",
      fornøyd_livet == "7" ~ "7",
      fornøyd_livet == "8" ~ "8",
      fornøyd_livet == "9" ~ "9",
      fornøyd_livet == "10 - SVÆRT FORNØYD" ~ "10",
      fornøyd_livet == "VET IKKE" ~ "VET IKKE"
    )
  ) %>%
  mutate(
    trygg_nærmiljø_kat = case_when(
      trygg_nærmiljø == "0 - IKKE TRYGG I DET HELE TATT" ~ "IKKE TRYGG",
      trygg_nærmiljø == "1" ~ "IKKE TRYGG",
      trygg_nærmiljø == "2" ~ "IKKE TRYGG",
      trygg_nærmiljø == "3" ~ "IKKE TRYGG",
      trygg_nærmiljø == "4" ~ "IKKE TRYGG",
      trygg_nærmiljø == "5" ~ "IKKE TRYGG",
      trygg_nærmiljø == "6" ~ "TRYGG",
      trygg_nærmiljø == "7" ~ "TRYGG",
      trygg_nærmiljø == "8" ~ "TRYGG",
      trygg_nærmiljø == "9" ~ "TRYGG",
      trygg_nærmiljø == "10 - SVÆRT TRYGG" ~ "TRYGG"
    )
  ) %>%
  mutate(
    utdanning_kat = case_when(
      utdanning == "Høy(Uni>2)" ~ "Høyere utdanning",
      utdanning == "Vgs/fag/høy/uni<2" ~ "Videregående skole",
      utdanning == "Grunnskole/tilsv." ~ "Grunnskole"
    )
  ) %>% 
  mutate(
    skader_kat = case_when(
      skader == "JA, EN" ~ "JA",
      skader == "JA, FLERE" ~ "JA",
      skader == "NEI" ~ "NEI"
    )
  ) %>% 
  mutate(
    tannlegebesøk_kat = case_when(
      tannlegebesøk == "0-2 ÅR SIDEN" ~ "NEI",
      tannlegebesøk == "3-5 ÅR SIDEN" ~ "JA",
      tannlegebesøk == "MER ENN 5 ÅR SIDEN" ~ "JA"
    )
  ) %>% 
  mutate(
    trivsel_nærmiljø_kat = case_when(
      trivsel_nærmiljø == "I STOR GRAD" ~ "I STOR GRAD",
      trivsel_nærmiljø == "I NOEN GRAD" ~ "MINDRE ENN STOR GRAD",
      trivsel_nærmiljø == "I LITEN GRAD" ~ "MINDRE ENN STOR GRAD",
      trivsel_nærmiljø == "IKKE I DET HELE TATT" ~ "MINDRE ENN STOR GRAD"
    )
  ) %>% 
  mutate(
    egen_tannhelse_kat = case_when(
      egen_tannhelse == "SVÆRT GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_tannhelse == "GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_tannhelse == "VERKEN GOD ELLER DÅRLIG" ~ "MINDRE ENN GOD",
      egen_tannhelse == "DÅRLIG" ~ "MINDRE ENN GOD",
      egen_tannhelse == "SVÆRT DÅRLIG" ~ "MINDRE ENN GOD"
    )
  ) %>% 
  mutate(
    kjønn = case_when(
      kjønn == "K" ~ "Kvinner",
      kjønn == "M" ~ "Menn",
    )
  )
 

#Filtrere på kun relevante variabler

df_kommuner <- kommune %>% select(kommunenr,
                                  alder,
                                  kjønn,
                                  utdanning_kat,
                                  trivsel_nærmiljø_kat,
                                  trygg_nærmiljø_kat,
                                  org_aktivitet_kat,
                                  annen_aktivitet_kat,
                                  egen_helse_kat,
                                  egen_tannhelse_kat,
                                  tannlegebesøk_kat,
                                  kost_bær_kat,
                                  kost_grønt_kat,
                                  skader_kat,
                                  fornøyd_livet_kat)


## Kommunevise datasett

df_vestfold <- df_kommuner %>% filter(kommunenr %in% c("3801", "3802", "3803", "3804", "3805", "3811"))
df_telemark <- df_kommuner %>% filter(kommunenr %in% c("3806","3807","3808","3812","3813","3814","3815","3816","3817","3818","3819","3820","3821","3822","3823","3824","3825"))

dfList <- list(df_vestfold, df_telemark)

##### Egen helse

egen_helse <- lapply(dfList, function(x) {
  egen_helse <- x %>% 
    group_by(egen_helse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    mutate(andel = round((n/sum(n)*100),1))
})

egen_helse_vestfold <- egen_helse[[1]]
egen_helse_telemark <- egen_helse[[2]]

write.table(egen_helse_vestfold, "output/egen_helse_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egen_helse_telemark, "output/egen_helse_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Egen helse X Kjønn og utdanningsnivå

egenhelse_utdanning <- lapply(dfList, function(x) {
  egenhelse_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(egen_helse_kat == "GOD ELLER SVÆRT GOD") %>% 
    select(-egen_helse_kat, -totalt)
  
  egenhelse_utdanning_pivot <- pivot_wider(egenhelse_utdanning, names_from = kjønn, values_from = andel)
  })

egenhelse_utdanning_vestfold <- egenhelse_utdanning[[1]]
egenhelse_utdanning_telemark <- egenhelse_utdanning[[2]]

write.table(egenhelse_utdanning_vestfold, "output/egenhelse_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egenhelse_utdanning_telemark, "output/egenhelse_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")



##### Bær

bær <- lapply(dfList, function(x) {
  bær <- x %>% 
    group_by(kost_bær_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    mutate(andel = round((n/sum(n)*100),1))
})

bær_vestfold <- bær[[1]]
bær_telemark <- bær[[2]]

write.table(bær_vestfold, "output/bær_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(bær_telemark, "output/bær_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


##### Grønnsaker

grønnsaker <- lapply(dfList, function(x) {
  grønnsaker <- x %>% 
    group_by(kost_grønt_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    mutate(andel = round((n/sum(n)*100),1))
})

grønnsaker_vestfold <- grønnsaker[[1]]
grønnsaker_telemark <- grønnsaker[[2]]

write.table(grønnsaker_vestfold, "output/grønnsaker_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(grønnsaker_telemark, "output/grønnsaker_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Grønnsaker X Kjønn og utdanningsnivå

kost_grønt_utdanning <- lapply(dfList, function(x) {
  kost_grønt_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, kost_grønt_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, kost_grønt_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(kost_grønt_kat == "Daglig") %>% 
    select(-kost_grønt_kat, -totalt)
  
  kost_grønt_utdanning_pivot <- pivot_wider(kost_grønt_utdanning, names_from = kjønn, values_from = andel)
})

kost_grønt_utdanning_vestfold <- kost_grønt_utdanning[[1]]
kost_grønt_utdanning_telemark <- kost_grønt_utdanning[[2]]

write.table(kost_grønt_utdanning_vestfold, "output/kost_grønt_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(kost_grønt_utdanning_telemark, "output/kost_grønt_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Organisert aktivitet X Kjønn og utdanningsnivå

org_aktivitet_utdanning <- lapply(dfList, function(x) {
  org_aktivitet_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, org_aktivitet_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, org_aktivitet_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(org_aktivitet_kat == "MINST ÉN GANG I UKA") %>% 
    select(-org_aktivitet_kat, -totalt)
  
  org_aktivitet_utdanning_pivot <- pivot_wider(org_aktivitet_utdanning, names_from = kjønn, values_from = andel)
})

org_aktivitet_utdanning_vestfold <- org_aktivitet_utdanning[[1]]
org_aktivitet_utdanning_telemark <- org_aktivitet_utdanning[[2]]

write.table(org_aktivitet_utdanning_vestfold, "output/org_aktivitet_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(org_aktivitet_utdanning_telemark, "output/org_aktivitet_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


##### Organisert aktivitet X Kjønn og ALDER

org_aktivitet_alder <- lapply(dfList, function(x) {
  org_aktivitet_alder <- x %>% 
    group_by(alder, kjønn, org_aktivitet_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(alder, kjønn, org_aktivitet_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(alder, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(org_aktivitet_kat == "MINST ÉN GANG I UKA") %>% 
    select(-org_aktivitet_kat, -totalt)
  
  org_aktivitet_alder_pivot <- pivot_wider(org_aktivitet_alder, names_from = kjønn, values_from = andel)
})

org_aktivitet_alder_vestfold <- org_aktivitet_alder[[1]]
org_aktivitet_alder_telemark <- org_aktivitet_alder[[2]]

write.table(org_aktivitet_alder_vestfold, "output/org_aktivitet_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(org_aktivitet_alder_telemark, "output/org_aktivitet_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")



##### Annen aktivitet X Kjønn og utdanningsnivå

annen_aktivitet_utdanning <- lapply(dfList, function(x) {
  annen_aktivitet_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, annen_aktivitet_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, annen_aktivitet_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(annen_aktivitet_kat == "MINST ÉN GANG I UKA") %>% 
    select(-annen_aktivitet_kat, -totalt)
  
  annen_aktivitet_utdanning_pivot <- pivot_wider(annen_aktivitet_utdanning, names_from = kjønn, values_from = andel)
})

annen_aktivitet_utdanning_vestfold <- annen_aktivitet_utdanning[[1]]
annen_aktivitet_utdanning_telemark <- annen_aktivitet_utdanning[[2]]

write.table(annen_aktivitet_utdanning_vestfold, "output/annen_aktivitet_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(annen_aktivitet_utdanning_telemark, "output/annen_aktivitet_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Egen tannhelse

egen_tannhelse <- lapply(dfList, function(x) {
  egen_tannhelse <- x %>% 
    group_by(egen_tannhelse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    mutate(andel = round((n/sum(n)*100),1))
})

egen_tannhelse_vestfold <- egen_tannhelse[[1]]
egen_tannhelse_telemark <- egen_tannhelse[[2]]

write.table(egen_tannhelse_vestfold, "output/egen_tannhelse_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egen_tannhelse_telemark, "output/egen_tannhelse_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


##### Egen tannhelse X Kjønn og utdanningsnivå

egen_tannhelse_utdanning <- lapply(dfList, function(x) {
  egen_tannhelse_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, egen_tannhelse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, egen_tannhelse_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(egen_tannhelse_kat == "GOD ELLER SVÆRT GOD") %>% 
    select(-egen_tannhelse_kat, -totalt)
  
  egen_tannhelse_utdanning_pivot <- pivot_wider(egen_tannhelse_utdanning, names_from = kjønn, values_from = andel)
})

egen_tannhelse_utdanning_vestfold <- egen_tannhelse_utdanning[[1]]
egen_tannhelse_utdanning_telemark <- egen_tannhelse_utdanning[[2]]

write.table(egen_tannhelse_utdanning_vestfold, "output/egen_tannhelse_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egen_tannhelse_utdanning_telemark, "output/egen_tannhelse_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


##### Egen tannhelse X Kjønn og alder

egen_tannhelse_alder <- lapply(dfList, function(x) {
  egen_tannhelse_alder <- x %>% 
    group_by(alder, kjønn, egen_tannhelse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(alder, kjønn, egen_tannhelse_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(alder, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(egen_tannhelse_kat == "GOD ELLER SVÆRT GOD") %>% 
    select(-egen_tannhelse_kat, -totalt)
  
  egen_tannhelse_alder_pivot <- pivot_wider(egen_tannhelse_alder, names_from = kjønn, values_from = andel)
})

egen_tannhelse_alder_vestfold <- egen_tannhelse_alder[[1]]
egen_tannhelse_alder_telemark <- egen_tannhelse_alder[[2]]

write.table(egen_tannhelse_alder_vestfold, "output/egen_tannhelse_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egen_tannhelse_alder_telemark, "output/egen_tannhelse_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Trivsel nærmiljø X Kjønn og alder

trivsel_nærmiljø_alder <- lapply(dfList, function(x) {
  trivsel_nærmiljø_alder <- x %>% 
    group_by(alder, kjønn, trivsel_nærmiljø_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(alder, kjønn, trivsel_nærmiljø_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(alder, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(trivsel_nærmiljø_kat == "I STOR GRAD") %>% 
    select(-trivsel_nærmiljø_kat, -totalt)
  
  trivsel_nærmiljø_alder_pivot <- pivot_wider(trivsel_nærmiljø_alder, names_from = kjønn, values_from = andel)
})

trivsel_nærmiljø_alder_vestfold <- trivsel_nærmiljø_alder[[1]]
trivsel_nærmiljø_alder_telemark <- trivsel_nærmiljø_alder[[2]]

write.table(trivsel_nærmiljø_alder_vestfold, "output/trivsel_nærmiljø_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(trivsel_nærmiljø_alder_telemark, "output/trivsel_nærmiljø_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


##### Skade X Kjønn og alder

skader_alder <- lapply(dfList, function(x) {
  skader_alder <- x %>% 
    group_by(alder, kjønn, skader_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(alder, kjønn, skader_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(alder, kjønn) %>% 
    mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
    filter(skader_kat == "JA") %>% 
    select(-skader_kat, -totalt)
  
  skader_alder_pivot <- pivot_wider(skader_alder, names_from = kjønn, values_from = andel)
})

skader_alder_vestfold <- skader_alder[[1]]
skader_alder_telemark <- skader_alder[[2]]

write.table(skader_alder_vestfold, "output/skader_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(skader_alder_telemark, "output/skader_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Trygg i nærmiljøet (6-10)

trygg_nærmiljø <- lapply(dfList, function(x) {
  trygg_nærmiljø <- x %>%
    group_by(trygg_nærmiljø_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>% 
    mutate(andel = round((n/sum(n)*100),1))
})

trygg_nærmiljø_vestfold <- trygg_nærmiljø[[1]]
trygg_nærmiljø_telemark <- trygg_nærmiljø[[2]]

write.table(trygg_nærmiljø_vestfold, "output/trygg_nærmiljø_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(trygg_nærmiljø_telemark, "output/trygg_nærmiljø_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Trygg i nærmiljøet (6-10) X Kjønn

trygg_nærmiljø_kjønn <- lapply(dfList, function(x) {
  trygg_nærmiljø_kjønn <- x %>%
    group_by(kjønn, trygg_nærmiljø_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    mutate(andel = round((n/sum(n)*100),1))
})

trygg_nærmiljø_kjønn_vestfold <- trygg_nærmiljø_kjønn[[1]]
trygg_nærmiljø_kjønn_telemark <- trygg_nærmiljø_kjønn[[2]]

write.table(trygg_nærmiljø_kjønn_vestfold, "output/trygg_nærmiljø_kjønn_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(trygg_nærmiljø_kjønn_telemark, "output/trygg_nærmiljø_kjønn_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

##### Fornøyd med livet (6-10)

fornøyd_livet <- lapply(dfList, function(x) {
  fornøyd_livet <- x %>%
  filter(fornøyd_livet_kat != "VET IKKE") %>%
  mutate(
    fornøyd_livet = case_when(
      fornøyd_livet_kat == "0" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "1" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "2" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "3" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "4" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "5" ~ "IKKE FORNØYD MED LIVET",
      fornøyd_livet_kat == "6" ~ "FORNØYD MED LIVET",
      fornøyd_livet_kat == "7" ~ "FORNØYD MED LIVET",
      fornøyd_livet_kat == "8" ~ "FORNØYD MED LIVET",
      fornøyd_livet_kat == "9" ~ "FORNØYD MED LIVET",
      fornøyd_livet_kat == "10" ~ "FORNØYD MED LIVET")) %>% 
  group_by(fornøyd_livet) %>% 
  summarise(n = n()) %>% 
  mutate(andel = round((n/sum(n)*100),1))
})

fornøyd_livet_vestfold <- fornøyd_livet[[1]]
fornøyd_livet_telemark <- fornøyd_livet[[2]]

write.table(fornøyd_livet_vestfold, "output/fornøyd_livet_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(fornøyd_livet_telemark, "output/fornøyd_livet_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")



##### Fornøyd med livet X Kjønn og alder

# Vestfold

fornøyd_livet_alder_vestfold <- df_vestfold %>% 
  filter(fornøyd_livet_kat != "VET IKKE") %>% 
  group_by(alder, kjønn) %>% 
  summarise(avg = round(mean(as.numeric(fornøyd_livet_kat)),2)) %>% 
  na.omit()

fornøyd_livet_alder_vestfold_pivot <- pivot_wider(fornøyd_livet_alder_vestfold, names_from = kjønn, values_from = avg)
write.table(fornøyd_livet_alder_vestfold_pivot, "output/fornøyd_livet_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

# Telemark

fornøyd_livet_alder_telemark <- df_telemark %>% 
  filter(fornøyd_livet_kat != "VET IKKE") %>% 
  group_by(alder, kjønn) %>% 
  summarise(avg = round(mean(as.numeric(fornøyd_livet_kat)),2)) %>% 
  na.omit()

fornøyd_livet_alder_telemark_pivot <- pivot_wider(fornøyd_livet_alder_telemark, names_from = kjønn, values_from = avg)
write.table(fornøyd_livet_alder_telemark_pivot, "output/fornøyd_livet_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

#### Tannlegebesøk x alder

# Vestfold

tannlegebesøk_alder_vestfold <- df_vestfold %>% 
  group_by(alder, tannlegebesøk_kat) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(alder, tannlegebesøk_kat) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(alder) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  filter(tannlegebesøk_kat == "JA") %>% 
  select(-tannlegebesøk_kat, -totalt)

write.table(tannlegebesøk_alder_vestfold, "output/tannlegebesøk_alder_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

# Telemark

tannlegebesøk_alder_telemark <- df_telemark %>% 
  group_by(alder, tannlegebesøk_kat) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(alder, tannlegebesøk_kat) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(alder) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  filter(tannlegebesøk_kat == "JA") %>% 
  select(-tannlegebesøk_kat, -totalt)

write.table(tannlegebesøk_alder_telemark, "output/tannlegebesøk_alder_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


#### Tannlegebesøk x utdanning

# Vestfold

tannlegebesøk_utdanning_vestfold <- df_vestfold %>% 
  group_by(utdanning_kat, tannlegebesøk_kat) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(utdanning_kat, tannlegebesøk_kat) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(utdanning_kat) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  filter(tannlegebesøk_kat == "JA") %>% 
  select(-tannlegebesøk_kat, -totalt)

write.table(tannlegebesøk_utdanning_vestfold, "output/tannlegebesøk_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

# Telemark

tannlegebesøk_utdanning_telemark <- df_telemark %>% 
  group_by(utdanning_kat, tannlegebesøk_kat) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(utdanning_kat, tannlegebesøk_kat) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(utdanning_kat) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  filter(tannlegebesøk_kat == "JA") %>% 
  select(-tannlegebesøk_kat, -totalt)

write.table(tannlegebesøk_utdanning_telemark, "output/tannlegebesøk_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


#### Tannhelse x Tid siden siste tannlegebesøk

df_tann <- kommune_raw %>% rename(egen_tannhelse = QFVT_2_2, tannlegebesøk = QFVT_2_3) %>%
  mutate(
    egen_tannhelse_kat = case_when(
      egen_tannhelse == "SVÆRT GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_tannhelse == "GOD" ~ "GOD ELLER SVÆRT GOD",
      egen_tannhelse == "VERKEN GOD ELLER DÅRLIG" ~ "VERKEN GOD ELLER DÅRLIG",
      egen_tannhelse == "DÅRLIG" ~ "DÅRLIG ELLER SVÆRT DÅRLIG",
      egen_tannhelse == "SVÆRT DÅRLIG" ~ "DÅRLIG ELLER SVÆRT DÅRLIG"
    )
  ) %>% 
  select(kommunenr, egen_tannhelse_kat, tannlegebesøk)

df_tann_v <- df_tann %>% filter(kommunenr %in% c("3801", "3802", "3803", "3804", "3805", "3811"))
df_tann_t <- df_tann %>% filter(kommunenr %in% c("3806","3807","3808","3812","3813","3814","3815","3816","3817","3818","3819","3820","3821","3822","3823","3824","3825"))

## Vestfold

tannhelse_tannbesøk_vestfold <- df_tann_v %>% 
  group_by(egen_tannhelse_kat, tannlegebesøk) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(egen_tannhelse_kat, tannlegebesøk) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(tannlegebesøk) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  select(-totalt)

tannhelse_tannbesøk_vestfold_pivot <- pivot_wider(tannhelse_tannbesøk_vestfold, names_from = egen_tannhelse_kat, values_from = andel)
write.table(tannhelse_tannbesøk_vestfold_pivot, "output/tannhelse_tannbesøk_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")

## Telemark

tannhelse_tannbesøk_telemark <- df_tann_t %>% 
  group_by(egen_tannhelse_kat, tannlegebesøk) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(egen_tannhelse_kat, tannlegebesøk) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(tannlegebesøk) %>% 
  mutate(andel = round((totalt/sum(totalt)*100),1)) %>% 
  select(-totalt)

tannhelse_tannbesøk_telemark_pivot <- pivot_wider(tannhelse_tannbesøk_telemark, names_from = egen_tannhelse_kat, values_from = andel)
write.table(tannhelse_tannbesøk_telemark_pivot, "output/tannhelse_tannbesøk_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")


