# baci-like-hs12

An attempt to create a consistent international trade dataset for the course STA2101.

I implemented a gravity-type model to remove transportation costs and mismatches by imputing around 10% 
of the exports data for the period 2002-2012, and also removed flows with no attributed origin/destination.

The idea of imputing some rows is because I applied a criteria of replacing the observed values with data from the model if and only if that reduced the mismatching between trading partners.

These tables discount re-imports and re-exports besides harmonizing mismatching flows.

This is similar to
[BACI: International Trade Database at the Product-Level. The 1994-2007 Version](http://www.cepii.fr/CEPII/fr/publications/wp/abstract.asp?NoDoc=2726).
