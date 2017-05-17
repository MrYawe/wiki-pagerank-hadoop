# wiki-pagerank-hadoop

## Starting
### New version
1) Create the `input_pagelinks` and `input_pages` folders at the root of the project.
2) Put `frwiki-latest-page.sql.gz` in `input_pages` and `frwiki-latest-pagelinks.sql.gz` in `input_pagelinks`.
3) Run the progam with 3 args: "input_pagelinks input_pages final_output". The `final_output` folder will be created automatically and musn't exist at start.

### Old version
This alpha version is based on this [blog article](http://blog.xebia.com/wiki-pagerank-with-hadoop/).
- create an `input` folder under the project root with your xml file on it. You can download it here [here](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles.xml.bz2).
- run the `WikiPageRanking` class with Intellij.


## Goal
The main goal is to improve the pagerank algorithm and to use SQL files instead of the huge XML file.

The relevant SQL files are:
- `frwiki-latest-page.sql.gz`
- `frwiki-latest-pagelinks.sql.gz`

They are provided by Wikipedia [here](https://dumps.wikimedia.org/frwiki/latest).
- [page.sql.gz](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-page.sql.gz)
- [pagelinks.sql.gz](https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pagelinks.sql.gz)

