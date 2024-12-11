let
    Kilde = List.Generate(  
        () => [x = Elhub_funksjon ("2021-01-01"), dato = "2021-01-01"],
    each Date.From ([dato]) < Date.From (DateTime.LocalNow ()),
    each [x = Elhub_funksjon(Date.ToText(Date.AddDays(Date.From([dato]), 1 ), [Format = "yyyy-MM-dd"])), 
    dato = Date.ToText(Date.AddDays(Date.From([dato]), 1 ), [Format = "yyyy-MM-dd"])],
    each [x]



    ),
    #"Konvertert til tabell" = Table.FromList(Kilde, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Utvidet Column1" = Table.ExpandTableColumn(#"Konvertert til tabell", "Column1", {"Knr", "Kommunenavn", "Tid", "Type", "KWh", "Antall målere"}, {"Knr", "Kommunenavn", "Tid", "Type", "KWh", "Antall målere"}),
    #"Endret type" = Table.TransformColumnTypes(#"Utvidet Column1",{{"Knr", Int64.Type}, {"KWh", type number}, {"Antall målere", Int64.Type}, {"Type", type text}, {"Kommunenavn", type text}, {"Tid", type datetime}}),
    #"Egendefinert lagt til" = Table.AddColumn(#"Endret type", "Forbruk (MWh)", each [KWh]/1000),
    #"Endret type1" = Table.TransformColumnTypes(#"Egendefinert lagt til",{{"Forbruk (MWh)", type number}}),
    #"Fjernede kolonner" = Table.RemoveColumns(#"Endret type1",{"KWh"}),
    #"Duplisert kolonne" = Table.DuplicateColumn(#"Fjernede kolonner", "Tid", "Tid - Kopier"),
    #"Uttrukket år" = Table.TransformColumns(#"Duplisert kolonne",{{"Tid - Kopier", Date.Year, Int64.Type}}),
    #"Duplisert kolonne1" = Table.DuplicateColumn(#"Uttrukket år", "Tid", "Tid - Kopier.1"),
    #"Beregnet slutt på måneden" = Table.TransformColumns(#"Duplisert kolonne1",{{"Tid - Kopier.1", Date.EndOfMonth, type datetime}}),
    #"Endret type2" = Table.TransformColumnTypes(#"Beregnet slutt på måneden",{{"Tid - Kopier.1", type date}}),
    #"Duplisert kolonne2" = Table.DuplicateColumn(#"Endret type2", "Tid", "Tid - Kopier.2"),
    #"Uttrukket dato" = Table.TransformColumns(#"Duplisert kolonne2",{{"Tid - Kopier.2", DateTime.Date, type date}}),
    #"Omorganiserte kolonner" = Table.ReorderColumns(#"Uttrukket dato",{"Knr", "Kommunenavn", "Type", "Antall målere", "Forbruk (MWh)", "Tid - Kopier", "Tid - Kopier.1", "Tid - Kopier.2", "Tid"}),
    #"Kolonner med nye navn" = Table.RenameColumns(#"Omorganiserte kolonner",{{"Tid - Kopier", "År"}, {"Tid - Kopier.1", "Måned og år"}, {"Tid - Kopier.2", "Dag"}, {"Tid", "Time"}}),
    #"Erstattet verdi" = Table.ReplaceValue(#"Kolonner med nye navn","business","Næring (untatt industri)",Replacer.ReplaceText,{"Type"}),
    #"Erstattet verdi1" = Table.ReplaceValue(#"Erstattet verdi","industry","Industri",Replacer.ReplaceText,{"Type"}),
    #"Erstattet verdi2" = Table.ReplaceValue(#"Erstattet verdi1","private","Husholdninger",Replacer.ReplaceText,{"Type"}),
    #"Kolonner med nye navn1" = Table.RenameColumns(#"Erstattet verdi2",{{"Type", "Gruppe"}})
in
    #"Kolonner med nye navn1"