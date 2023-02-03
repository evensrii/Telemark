library(tidyverse)

theme_set(theme_bw()) #set ggplot2 theme
setwd("C:/Users/eve1509/OneDrive - Vestfold og Telemark fylkeskommune/Github/Telemark/Data/08_Folkehelse og levekår/R/")
getwd()

kommune_raw <- read_csv("input/V&T.csv") #Datasett med kommuner
#land <- read_csv("input/V&Tlandgr.csv") #Info om landbakgrunn, men ikke kommune

# Resultatene er samlet i/eksportert til et eget Excel-ark, "Tallgrunnlag tannhelse.xlsx" i FHUS-teamet, "C:\Users\eve1509\Vestfold og Telemark fylkeskommune\OF-Folkehelseundersøkelsen i Vestfold og Telemark - General"

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

## Rename variabler

kommune <- kommune_raw %>% rename(alder = alderkat,
                            kjønn = Kjonn_Kode,
                            utdanning = utdannelse,
                            trivsel_nærmiljø = QFVT_1_1,
                            org_aktivitet = QFVT_1_9,
                            annen_aktivitet = QFVT_1_10,
                            egen_helse = QFVT_2_1,
                            egen_tannhelse = QFVT_2_2,
                            tannlegebesøk = QFVT_2_3,
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
      kjønn == "K" ~ "Kvinne",
      kjønn == "M" ~ "Mann",
    )
  )
 

#Filtrere på kun relevante variabler

df_kommuner <- kommune %>% select(kommunenr,
                                  alder,
                                  kjønn,
                                  utdanning_kat,
                                  trivsel_nærmiljø_kat,
                                  org_aktivitet_kat,
                                  annen_aktivitet_kat,
                                  egen_helse_kat,
                                  egen_tannhelse_kat,
                                  tannlegebesøk_kat,
                                  kost_grønt_kat,
                                  skader_kat,
                                  fornøyd_livet_kat)


## Kommunevise datasett

df_vestfold <- df_kommuner %>% filter(kommunenr %in% c("3801", "3802", "3803", "3804", "3805", "3811"))
df_telemark <- df_kommuner %>% filter(kommunenr %in% c("3806","3807","3808","3812","3813","3814","3815","3816","3817","3818","3819","3820","3821","3822","3823","3824","3825"))

dfList <- list(df_vestfold, df_telemark)

##### EGEN HELSE X UTDANNINGSNIVÅ

egenhelse_utdanning <- lapply(dfList, function(x) {
  egenhelse_utdanning <- x %>% 
    group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
    summarise(n = n()) %>% 
    na.omit() %>%
    group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
    summarise(totalt = sum(n)) %>% 
    group_by(utdanning_kat, kjønn) %>% 
    mutate(andel = totalt/sum(totalt)*100) %>% 
    filter(egen_helse_kat == "GOD ELLER SVÆRT GOD") %>% 
    select(-egen_helse_kat, -totalt)
  
  egenhelse_utdanning_pivot <- pivot_wider(egenhelse_utdanning, names_from = kjønn, values_from = andel)
  })

egenhelse_utdanning_vestfold <- egenhelse_utdanning[[1]]
egenhelse_utdanning_telemark <- egenhelse_utdanning[[2]]

write.table(egenhelse_utdanning_vestfold, "output/egenhelse_utdanning_vestfold.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")
write.table(egenhelse_utdanning_telemark, "output/egenhelse_utdanning_telemark.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")






#Antall NA
#unique(df_kommuner$utdanning_kat)
#nrow(df_kommuner[is.na(df_kommuner$utdanning_kat),])

egenhelse_utdanning <- df_vestfold %>% 
  group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
  summarise(n = n()) %>% 
  na.omit() %>%
  group_by(utdanning_kat, kjønn, egen_helse_kat) %>% 
  summarise(totalt = sum(n)) %>% 
  group_by(utdanning_kat, kjønn) %>% 
  mutate(andel = totalt/sum(totalt))

## Renske tabell



#Konverere til (og endre rekkefølge på) faktorer
tannhelse_utdanning$god_dårlig <- as.factor(tannhelse_utdanning$god_dårlig)
tannhelse_utdanning$utdannelse <- as.factor(tannhelse_utdanning$utdannelse)
tannhelse_utdanning$utdannelse <- factor(tannhelse_utdanning$utdannelse, levels = c("Grunnskole/tilsv.","Vgs/fag/høy/uni<2", "Høy(Uni>2)"))

#Plotte
ggplot(tannhelse_utdanning, aes(fill=god_dårlig, y=andel, x=utdannelse)) + 
  geom_bar(position="dodge", stat="identity") +
  geom_text(aes(label=andel), vjust = -0.5, position = position_dodge(.9))

#Pivotere og skrive til .csv
tannhelse_utdanning_pivot <- pivot_wider(tannhelse_utdanning, names_from = utdannelse, values_from = andel)
write.table(tannhelse_utdanning_pivot, "output/tannhelse_utdanning.csv", sep = ";", dec = ",", quote = FALSE, row.names = FALSE, col.names = TRUE, fileEncoding = "utf8")





