I need to plot many of the different results as graphs, and output them as png files in "Microdata\09_Innvandrere og integrering\Skien\Flytting til og fra Skien\Graphs". The results are stored as .csv files in the "Microdata\09_Innvandrere og integrering\Skien\Flytting til og fra Skien" folder, and each results consists of a title followed by a table.

For example:

Familieinnvandret per år:

| fam_flytteaar |    |
|---------------+----|
|          2020 | 17 |
|          2021 | 14 |
|          2022 | 17 |
|          2023 |  9 |
|          2024 |  9 |
|         Total | 67 |

I need bar graphs based on the tables with the following titles:

Innflyttede innvandrere etter kategori
Innflytting til Skien
Alder ved innflytting (antall og prosent)
Fødeland innflyttere
Innvandringsbakgrunn innflyttere (antall og prosent)
Utflytting fra Skien
Alder ved utflytting (antall og prosent)
Fødeland utflyttere
Innvandringsgrunn innflyttere (antall og prosent)
Sekundærbosatt botid
Sekundærbosatt alder (antall og prosent)
Sekundærbosatt landbakgrunn
Sekundærbosatt fra fylke
Sekundærbosatt utdanningsnivå 16 år og eldre (antall og prosent)
Sekundærbosatt husholdningstype
Familieinnvandret per år
Familieinnvandret etter alder

For all graphs, I would like to remove any numerical parts or category codes in the start of the category name. For example:
"1 - Innflyttere, ikke innvandrere" ---> "Innflyttere, ikke innvandrere".
"534 - Pakistan" ---> "Pakistan"
"ANN - Annet" ---> "Annet"

For the graphs with fødeland, plot only the top 15.

In cases where there are both antall and prosent, use the absolute number, but label the bars with the percentage from the table (not calculated anew from the absolute numbers), rounded to the closest integer (i.e. 18 %).

Note that the antall and prosent numbers are separated by a blank space, and that the decimal in the percentage is a comma.

Is the task clear? Should you maybe try with one or two datasets first?
