# Russian Arctic Vegetation Archive open-access data

The goal of the Russian Arctic Vegetation Archive (AVA-RU) is to unite and harmonize data of plot-based plant species and their abundance,
vegetation structure and environmental variables from the Russian Arctic (including historical Soviet data). This database can be used to assess the status of the Russian Arctic vegetation and as a baseline to document biodiversity changes in the future. The archive can be used for scientific studies as well as to inform nature protection and restoration efforts.

## Description of the Data and file structure

Each AVA-RU dataset consists of 2 files:

*   Note: The species data tables consist of the header (rows 1-10) and species data (rows 10+). The header data contains some empty cells, including a completely empty row 7 that is added for better visual readability. It is recommended to remove rows 1-7 of the header before doing quantitative analysis.
*   Habitat data: CSV file containing a corresponding ecotope and
    community-structure data. Note: Empty cells in habitat data tables should be interpreted as 'NA'.

The habitat data codes and abbreviations follow: <https://arcticatlas.geobotany.org/catalog/dataset/current-turboveg-data-dictionary-and-panarctic-species-list-pasl>
(Alaska Arctic Vegetation Archive data dictionary: Tables (Main table)). The detailed information on the data structure can be found in the Appendix 1 of the paper.

Used abbreviations:

1.  Y - Yes, N - No
2.  Shape of the relev area: square (S), rectangle (R), linear/band-forming (L), circular (O), irregular (I), more subplots combined (C), unknown (not-recorded) (N). (Mucina et al. 2009)
3.  Cover abundance scale: Percentage (%) (00); Braun/Blanquet (old) (01); Braun/Blanquet (new) (02); Londo (03); Presence/Absence (04); Ordinal scale (1-9) (05); Barkman, Doing & Segal (06); Doing (07), Constancy classes (08), Domin (09), Colin (10), Tansley (11), Didukh (12), Hult-Sernander-Du Rietz (Daniels) (13), Braun-Blanquet (enlarge) (14), Westhoff & van der Maarel (15), Numbers (<65025) (98), Numbers (<24000) (99).
4.  Method used to collect vegetation-plot data: relev (R), other (O).
5.  Comm system: Braun-Blanquet (B-B), USNVC name (USNVC), CNVC name (CNVC), Russian nomenclature system (RU), field community name (FLD_NM).
6.  Arctic tundra bioclimate zone: Subzone A (A), Subzone B (B), Subzone C (C), Subzone D (D), Subzone E (E), Treeless Oceanic Boreal (O), Forest-Tundra Transition (FT), Boreal (BO). (CAVM Team 2003)
7.  Aspect of relev (degrees). Aspect is measured counterclockwise in degrees from 0 (due north) to 360 (again due north, coming full circle). As a convention, use 360 degrees for north. NNE (23), NE (45), ENE (68), E (90), ESE (113), SE (135), SSE (158), S (180), SSW (203), SW (225), WSW (248), W (270), WNW (293), NW (315), NNW (338), N (360), too flat to determine (-1), too irregular to determine (-2).
8.  Topographic position: flat elevated plain (includes plateaus and elevated river terraces) (EL_PLN); hill crest (CRST); shoulder (SHLD); backslope (BACK); footslope (includes toeslopes) (FOOT); flat low plain (LW_PLN); riparian zone (includes active floodplains, drainage channels, water tracks) (RIPZN); lake or pond (LAKE).
9.  Site moisture: dry (DRY), moist (MST), wet (WET), aquatic/emergent (AQU), unknown (not recorded) (N).
10. Disturbance: natural vegetation (NAT) or anthropogenically disturbed (DIS).
11. Soil texture of top mineral horizon: gravel (GRV), sand (SND), silt (SLT), clay (CLY), loam (LOM), organic (if no mineral soil within the active layer) (ORG).
12. Subjective assessment of floristic (vascular, cryptogam) quality: highest (1), high (2), high but incomplete (3), moderate (4), moderate and incomplete (5), low (6).

The detailed description of the methodology as well as a full data dictionary could be found in the paper:

Zemlianskii, V., Ermokhina, K., Schaepman-Strub, G., Matveyeva, N., Troeva, E., Lavrinenko, I., Telyatnikov, M., Pospelov, I., Koroleva, N., Leonova, N., Khitun, O., Walker D., Breen A., Kadetov, N. Lavrinenko, O., Ivleva T., Kholod, S., Petrzhik, N., Gunin, Y., Kurysheva, M., Lapina. A., Korolev, D., Kudr, E. & Plekhanova, E., (2023). Russian Arctic Vegetation Archive -- a new
database of plant community composition and environmental conditions.
Global Ecology and Biogeography, (issue number and pages).

## Sharing/access Information

Data and data statistics are also available at <https://avarus.space/>
