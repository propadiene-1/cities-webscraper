## EU Municipal Elections Scraper

webscraping work for wellesley polisci

## File structure

- process_less_1000.py: for <1000 communes
- process_plus_1000.py: for 1000+ communes
- utils.py: tools for above functions
- test.py: test candidate info (compare csv w/ results source file)

- france_2020
    - tour_1
        - ... 1000-et-plus.txt = results for 1000+ communes (\t separator)
        - ... moins-de-1000.txt = results for <1000 communes 
    - tour_2 
        - ... 1000-hab-et-plus.txt = results for 1000+ communes
        - ... moins-de-1000-hab.txt = results for <1000 communes
    - candidats-13-03-2020.csv = candidate registration info (name, sex, etc.)
    - notes
        - combines reg. w/ results
        - gender scraped from prefix (ms/mr)
- france_2014
    - tour_1 
        - one results file
    - tour_2
        - one results file
    - notes
        - no candidate registration info (nonexistent for 1000+, 28 separate files for <1000)
        - candidate name, gender, etc. obtained from results files

- read_census.py: parse big census file (not working rn)

## Notes

- separate files for tour 1 vs. tour 2
- separate parsing for large vs. small communes (<1000 vs. 1000+)
- no age data anywhere
- elections in france have tour 1, and tour 2, not everyone makes it to tour 2 (tour 2 results only has ppl who made it)
- large communes (1000+ people) vote for lists instead of individuals, candidate's vote # = the vote # of the whole list