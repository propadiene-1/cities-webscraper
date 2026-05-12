## EU Municipal Elections Scraper

webscraping work for wellesley polisci

## Notes

- separate files for tour 1 vs. tour 2
- separate parsing for large vs. small communes (<1000 vs. 1000+)
- no age data anywhere
- elections in france have tour 1, and tour 2, not everyone makes it to tour 2 (tour 2 results only has ppl who made it)
- large communes (1000+ people) vote for lists instead of individuals, candidate's vote # = the vote # of the whole list
- PLM (paris, lyon, marseille) vote by arrondissment (subdivisions w/ unique "SR" commune codes)

- checks
    - cross_check.py: cross check candidate outputs
    - check_joined_outputs.py: cross_check outputs merged w/ demographics
    - test.py: test candidate info (compare csv w/ results source file)
